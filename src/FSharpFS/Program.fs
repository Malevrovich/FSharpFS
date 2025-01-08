open Tmds.Fuse
open FSFS
open System.IO
open Tmds.Linux

open FSFS.FileSystem
open FSFS.FileSystemPersistence

let formatFile filename = 
    use file = File.Open(filename, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite)
    file.Position <- 0l
    file |> formatStream 1500u 1500u 200u |> ignore

let openFS filename = 
    let rec openHelper filename retry = 
        let maybeFileSystem = tryReadFileSystem filename

        match maybeFileSystem with
        | Some fileSystem -> Ok fileSystem
        | None -> 
            formatFile filename
            if retry then
                openHelper filename false
            else
                Error "Failed to open file after format!"
            
    openHelper filename true

[<EntryPoint>]
let main argv =
    if not (Fuse.CheckDependencies()) then
        printfn "%s" Fuse.InstallationInstructions
    else
        printfn "Opening file %s" argv[0]

        let fsRes = openFS argv[0]

        match fsRes with
        | Error err -> 
            printfn "Error: %s" err
        | Ok (fileSystem) ->
            let fs = new FSFSFileSystem(fileSystem) :> IFuseFileSystem // fsharplint:disable-line. new indicates that type is IDisposable

            let mountPoint = "/tmp/fsfs"

            printfn "Mounting FSFSFileSystem at %s" mountPoint

            Fuse.LazyUnmount(mountPoint)
            Directory.CreateDirectory(mountPoint) |> ignore

            let mountOptions = MountOptions()
            mountOptions.Options <- "default_permissions,allow_root"

            try
                using (Fuse.Mount(mountPoint, fs, mountOptions)) (fun mount ->
                    printfn "Mount completed"
                    mount.WaitForUnmountAsync() |> Async.AwaitTask |> Async.RunSynchronously)
            with :? FuseException as ex ->
                printfn "Fuse throw an exception: %A" ex
                printfn "Try unmounting the file system by executing:"
                printfn "fuser -kM %s" mountPoint
                printfn "sudo umount -f %s" mountPoint

    // let mkDirResult = 
    //     fsRes
    //     |> Result.bind (
    //         fun fs -> 
    //             printfn "Opened fs: %A" fs.FileTree

    //             fs 
    //             |> createDirectory "/dir1" 0u 0u (0b111_111_111us |> mode_t.op_Implicit)
    //             |> Result.mapError (fun code -> sprintf "Directory creation failed with %d" code)
    //     )
    
    // match mkDirResult with
    // | Ok () -> printfn "Success!"
    // | Error (what) -> printfn "Error: %s" what

    0


