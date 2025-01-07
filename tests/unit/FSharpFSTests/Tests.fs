module Tests

open System
open Xunit

open FSFS.Allocator

[<Fact>]
let ``Base Allocator Check`` () =
    let allocator = BlockAllocator(200)

    let blocks =
        [ 64; 65; 66; 5 ]
        |> Seq.collect (fun x -> allocator.Allocate(x).Value)
        |> Seq.sort
        |> List.ofSeq

    Assert.Equal(blocks.Length, 200)
    blocks |> Seq.iteri (fun idx block -> Assert.Equal(idx, block))

    let freeBlocks = List.init 19 (fun idx -> idx * 10)
    allocator.Free(freeBlocks)

    seq { 1..19 } |> Seq.iter (fun x -> allocator.Free(allocator.Allocate(x).Value))

    let allocatedBlocks = allocator.Allocate(19).Value |> List.sort

    Assert.Equal(allocatedBlocks.Length, 19)

    List.zip allocatedBlocks freeBlocks
    |> List.iter (fun (allocated, free) -> Assert.Equal(allocated, free))

    allocator.Free(seq { 0..199 })

    let allBlocks = allocator.Allocate(200).Value |> List.sort

    Assert.True(allocator.Allocate(1) |> Option.isNone)

    Assert.Equal(allBlocks.Length, 200)
    allBlocks |> Seq.iteri (fun idx block -> Assert.Equal(idx, block))

    allocator.Free(seq { 0..199 })

[<Fact>]
let ``Concurrent small block allocate`` () =
    let workers = 10000
    let allocator = BlockAllocator(workers)

    let allocateSeq cnt =
        let stopWatch = System.Diagnostics.Stopwatch.StartNew()

        let res =
            Seq.init cnt (fun _ -> async { return allocator.Allocate(1).Value })
            |> Async.Parallel
            |> Async.RunSynchronously

        stopWatch.Stop()

        let blocks = res |> Seq.concat |> List.ofSeq

        Assert.Equal(blocks.Length, cnt)
        blocks |> Seq.sort |> Seq.iteri (fun idx block -> Assert.Equal(idx, block))

        Assert.Equal(allocator.Available(), 0)

        printfn "Allocated %d elements in %f ms" cnt stopWatch.Elapsed.TotalMilliseconds

    allocateSeq workers

    Seq.init workers (fun idx -> async { allocator.Free([ idx ]) })
    |> Async.Parallel
    |> Async.RunSynchronously
    |> ignore

    allocateSeq workers
