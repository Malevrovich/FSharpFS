module FSFS.BitmapAllocatorPersistence

open System.IO

open System
open System.Threading
open FSharp.Span.Utils

open FSFS.BitmapAllocator
open FSFS.ObjectPool

type BitmapPersisterPool(filename: string, initialState: Bitmap, allocator: BitmapAllocator, baseOffset: uint32) =
    let filePool =
        new ObjectPool<FileStream>(
            16u,
            (fun _ -> File.Open(filename, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite)),
            (fun file -> file.Close() |> ignore)
        )

    let state = initialState |> Array.copy

    let storeBlock bitmaskIdx =
        let handle, stream = filePool.Acquire()

        stream.Position <- int64 (baseOffset + bitmaskIdx * uint sizeof<uint64>)

        let writeData = BitConverter.GetBytes(state[int bitmaskIdx])
        stream.Write(writeData)
        stream.Flush(true)

        filePool.Release(handle)

    interface IDisposable with
        member this.Dispose() = (filePool :> IDisposable).Dispose()

    member this.PersistBlockAllocation(block) =
        let bitmaskIdx, mask = allocator.BlockMask(block)
        Interlocked.Or(&state[int bitmaskIdx], mask) |> ignore
        storeBlock bitmaskIdx

    member this.PersistBlockRelease(block) =
        let bitmaskIdx, mask = allocator.BlockMask(block)
        Interlocked.And(&state[int bitmaskIdx], ~~~mask) |> ignore
        storeBlock bitmaskIdx

type PersistableBitmapAllocator =
    { Allocator: BitmapAllocator
      Persister: BitmapPersisterPool }

    interface IDisposable with
        member this.Dispose() =
            (this.Persister :> IDisposable).Dispose()

let serializeState (state: Bitmap) (stream: Stream) =
    let bytes = state |> Seq.map (BitConverter.GetBytes) |> Array.concat
    stream.Write(readonlyspan bytes)
    stream

let serializeAllocator (persistableAllocator: PersistableBitmapAllocator) (stream: Stream) =
    let allocator = persistableAllocator.Allocator
    stream |> serializeState (allocator.State())

let deserializeAllocator (blockCount: uint) (filename: string) (stream: Stream) =
    let position = stream.Position

    let bytes =
        Seq.init (bitmapLen (int blockCount)) (fun _ -> 0UL)
        |> Seq.map (BitConverter.GetBytes)
        |> Array.concat

    stream.Read(span bytes) |> ignore

    let state =
        bytes
        |> Seq.chunkBySize sizeof<uint64>
        |> Seq.map BitConverter.ToUInt64
        |> Array.ofSeq

    let allocator = BitmapAllocator(state, blockCount)

    { Allocator = allocator
      Persister = new BitmapPersisterPool(filename, allocator.State(), allocator, uint32 position) }
