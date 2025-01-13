module FSFS.ObjectPool

open System.Threading

open FSFS.BitmapAllocator
open System

type ObjectHandle = private { Index: uint }

type ObjectPool<'T>(objNum, (intializer: int -> 'T), ?cleaner) =
    let objects = Array.init (int objNum) intializer

    let allocator = BitmapAllocator(objNum)

    let semaphore = new SemaphoreSlim(int objNum)

    interface IDisposable with
        member this.Dispose() : unit =
            semaphore.Dispose()

            match cleaner with
            | Some cleanerFn -> objects |> Array.iter cleanerFn
            | _ -> ()

    member this.Acquire() =
        semaphore.Wait()
        let handle = { Index = allocator.Allocate(1u) |> Option.get |> List.exactlyOne }
        handle, objects[int handle.Index]

    member this.Release(handle) =
        allocator.Free([ handle.Index ])
        semaphore.Release() |> ignore
