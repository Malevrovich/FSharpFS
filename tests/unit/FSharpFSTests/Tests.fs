module Tests

open Xunit
open FsToolkit.ErrorHandling
open Tmds.Linux

open FSFS.BitmapAllocator
open FSFS.ObjectPool
open FSFS.FileSystemPersistence
open FSFS.FileTree

[<Fact>]
let ``Base Allocator Check`` () =
    let allocator = BitmapAllocator(200u)

    let blocks =
        [ 64u; 65u; 66u; 5u ]
        |> Seq.collect (fun x -> allocator.Allocate(x).Value)
        |> Seq.sort
        |> List.ofSeq

    Assert.Equal(blocks.Length, 200)
    blocks |> Seq.iteri (fun idx block -> Assert.Equal(uint idx, block))

    let freeBlocks = List.init 19 (fun idx -> uint idx * 10u)
    allocator.Free(freeBlocks)

    seq { 1u..19u } |> Seq.iter (fun x -> allocator.Free(allocator.Allocate(x).Value))

    let allocatedBlocks = allocator.Allocate(19u).Value |> List.sort

    Assert.Equal(allocatedBlocks.Length, 19)

    List.zip allocatedBlocks freeBlocks
    |> List.iter (fun (allocated, free) -> Assert.Equal(allocated, free))

    allocator.Free(seq { 0u..199u })

    let allBlocks = allocator.Allocate(200u).Value |> List.sort

    Assert.True(allocator.Allocate(1u) |> Option.isNone)

    Assert.Equal(allBlocks.Length, 200)
    allBlocks |> Seq.iteri (fun idx block -> Assert.Equal(uint idx, block))

    allocator.Free(seq { 0u..199u })

[<Fact>]
let ``Concurrent small block allocate`` () =
    let workers = 10000
    let allocator = BitmapAllocator(uint workers)

    let allocateSeq cnt =
        let stopWatch = System.Diagnostics.Stopwatch.StartNew()

        let res =
            Seq.init cnt (fun _ -> async { return allocator.Allocate(1u).Value })
            |> Async.Parallel
            |> Async.RunSynchronously

        stopWatch.Stop()

        let blocks = res |> Seq.concat |> List.ofSeq

        Assert.Equal(blocks.Length, cnt)
        blocks |> Seq.sort |> Seq.iteri (fun idx block -> Assert.Equal(uint idx, block))

        Assert.Equal(allocator.Available(), 0)

        printfn "Allocated %d elements in %f ms" cnt stopWatch.Elapsed.TotalMilliseconds

    allocateSeq workers

    Seq.init workers (fun idx -> async { allocator.Free([ uint idx ]) })
    |> Async.Parallel
    |> Async.RunSynchronously
    |> ignore

    allocateSeq workers

[<Fact>]
let ``Concurrent object pool usage``() = 
    let workers = 10000
    let poolSize = 64

    let arr = Array.init poolSize (fun _ -> 0)
    
    let incrementer idx = arr[idx] <- arr[idx] + 1

    use incPool = new ObjectPool<unit -> unit>(uint poolSize, fun x -> (fun () -> incrementer x))

    Seq.init workers (fun idx -> async {
        let handle, inc = incPool.Acquire()
        inc()
        incPool.Release(handle)
    })
    |> Async.Parallel
    |> Async.RunSynchronously
    |> ignore

    Assert.Equal(workers, arr |> Array.reduce (+))

[<Fact>]
let ``Basic file tree actions``() = 
    let fileTree = DirectoryNode(createRootNode {BlockId = 0u} {BlockId = 0u})

    let createDir name = DirectoryNode({
        Common = {
            Name = {BlockId = 0u}, name
            Addr = {BlockId = 1u}
            Uid = 0u
            Gid = 0u
            Mode = (0b111_111_111us |> mode_t.op_Implicit) // rwxr-xr-x
        }
        Content = Map.empty
    })

    let addDir (path: string) fileTree = 
        let lastSep = path.LastIndexOf('/') + 1
        let dirPath = path.Substring(0, lastSep)
        let filename = path.Substring(lastSep)

        // printfn "dirPath %s filename %s" dirPath filename
        
        fileTree |> tryAddNodeAt dirPath (createDir filename)
    
    let finalTreeRes = result {
        let! tree = fileTree |> addDir "/a"
        printfn "Created /a"
        let! tree2 = snd tree |>  addDir "/b"
        printfn "Created /b"
        let! tree3 = snd tree2 |>  addDir "/c"
        printfn "Created /c"
        let! tree4 = snd tree3 |>  addDir "/a/d"
        printfn "Created /a/d"
        return tree4
    }

    let assertNotError res = 
        match res with
        | Ok(value) -> 
            value
        | Error(code) -> 
            failwith (sprintf "Failed with code %d" code)

    let assertDirectory res =
        match res with
        | FileNode(_) as f -> failwith (sprintf "Unexpected file %A" f)
        | FillerNode(_) as f -> failwith (sprintf "Unexpected filler %A" f)
        | DirectoryNode(dirMD) -> dirMD

    let (_, finalTree) = assertNotError finalTreeRes
    let findA = finalTree |> tryGetNodeAt "/a" |> assertNotError |> assertDirectory

    Assert.Equal (
        snd findA.Common.Name,
        "a"
    )

    let findB = finalTree |> tryGetNodeAt "/b" |> assertNotError |> assertDirectory

    Assert.Equal (
        snd findB.Common.Name,
        "b"
    )

    let findC = finalTree |> tryGetNodeAt "/c" |> assertNotError |> assertDirectory

    Assert.Equal (
        snd findC.Common.Name,
        "c"
    )

    let findD = finalTree |> tryGetNodeAt "/a/d" |> assertNotError |> assertDirectory

    Assert.Equal (
        snd findD.Common.Name,
        "d"
    )

[<EntryPoint>]
let main argv = 0