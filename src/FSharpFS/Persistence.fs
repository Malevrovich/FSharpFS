module FSFS.Persistence

open System.IO
open MBrace.FsPickler
open FSharp.Span.Utils

open FSFS.ObjectPool
open System

type BlockStorageAddr = { BlockId: uint32 } // block id

let blockId = fun blockAddr -> blockAddr.BlockId

let private binarySerializer = FsPickler.CreateBinarySerializer()

let serialize<'T> obj stream =
    binarySerializer.Serialize<'T>(stream, obj, leaveOpen = true)
    stream

let serializeWith<'T> obj pickler stream =
    binarySerializer.Serialize<'T>(stream, obj, pickler, leaveOpen = true)
    stream

let deserialize<'T> stream =
    binarySerializer.Deserialize<'T>(stream, leaveOpen = true)

let tryDeserialize<'T> stream =
    try
        deserialize<'T> stream |> Some
    with :? FsPicklerException ->
        None

let deserializeWith<'T> stream pickler =
    binarySerializer.Deserialize<'T>(stream, pickler, leaveOpen = true)

type private BlockAddressableStream =
    { Stream: Stream
      BlockSize: uint32
      BaseOffset: uint32 }

let private addrToPos blockStream addr = addr.BlockId * blockStream.BlockSize

let private posToAddr blockStream pos =
    { BlockStorageAddr.BlockId = pos / blockStream.BlockSize }

let private setPosition pos blockStream =
    blockStream.Stream.Position <- int64 (pos + blockStream.BaseOffset)

let private setAddr addr offset blockStream =
    blockStream |> setPosition ((addr |> addrToPos blockStream) + offset)


type ObjectBlockStorageIOAdapter(stream: Stream, blockSize: uint32, baseOffset: uint32) =
    let blockStream =
        { Stream = stream
          BlockSize = blockSize
          BaseOffset = baseOffset }

    member this.ReadObject<'T> addr =
        blockStream |> setAddr addr 0u
        binarySerializer.Deserialize<'T>(blockStream.Stream, leaveOpen = true)

    member this.WriteObject<'T> (value: 'T) addr =
        blockStream |> setAddr addr 0u
        binarySerializer.Serialize(blockStream.Stream, value, leaveOpen = true)
        blockStream.Stream.Flush() |> ignore


type DataBlockStorageIOAdapter(stream: Stream, blockSize: uint32, baseOffset: uint32) =
    let blockStream =
        { Stream = stream
          BlockSize = blockSize
          BaseOffset = baseOffset }

    let blockOffset maybeOffset =
        maybeOffset
        |> Option.bind (fun x -> if x > 0u then Some x else None)
        |> Option.defaultValue 0u

    let setBlockAddr blockAddr offset =
        blockStream |> setAddr blockAddr (offset |> blockOffset)

    member this.ReadData(blockAddr, span: byte span, ?offset) =
        setBlockAddr blockAddr offset
        blockStream.Stream.Read(span) |> ignore

    member this.WriteData(blockAddr, span: byte readonlyspan, ?offset) =
        setBlockAddr blockAddr offset
        blockStream.Stream.Write(span)
        blockStream.Stream.Flush() |> ignore

type ObjectBlockStorageIOAdapterPool(filename: string, blockSize: uint32, baseOffset: uint32) =
    let filePool =
        new ObjectPool<Stream * ObjectBlockStorageIOAdapter>(
            16u,
            (fun _ ->
                let file =
                    File.Open(filename, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite)

                file, ObjectBlockStorageIOAdapter(file, blockSize, baseOffset)),
            (fun (file, _) -> file.Close() |> ignore)
        )

    interface IDisposable with
        member this.Dispose() = (filePool :> IDisposable).Dispose()

    member this.BaseOffset = baseOffset


    member this.ReadObject<'T> addr =
        let (handle, (file, io)) = filePool.Acquire()
        let res = io.ReadObject<'T> addr
        filePool.Release handle
        res

    member this.WriteObject<'T> (value: 'T) addr =
        let (handle, (file, io)) = filePool.Acquire()
        let res = io.WriteObject<'T> value addr
        filePool.Release handle

type DataBlockStorageIOAdapterPool(filename: string, blockSize: uint32, baseOffset: uint32) =
    let filePool =
        new ObjectPool<Stream * DataBlockStorageIOAdapter>(
            16u,
            (fun _ ->
                let file =
                    File.Open(filename, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite)

                file, DataBlockStorageIOAdapter(file, blockSize, baseOffset)),
            (fun (file, _) -> file.Close() |> ignore)
        )

    interface IDisposable with
        member this.Dispose() = (filePool :> IDisposable).Dispose()

    member this.BaseOffset = baseOffset

    member this.ReadData(blockAddr, span: byte span, ?offset) =
        let (handle, (file, io)) = filePool.Acquire()

        match offset with
        | Some offset -> io.ReadData(blockAddr, span, offset)
        | None -> io.ReadData(blockAddr, span)

        filePool.Release handle

    member this.WriteData(blockAddr, span: byte readonlyspan, ?offset) =
        let (handle, (file, io)) = filePool.Acquire()

        match offset with
        | Some offset -> io.WriteData(blockAddr, span, offset)
        | None -> io.WriteData(blockAddr, span)

        filePool.Release handle
