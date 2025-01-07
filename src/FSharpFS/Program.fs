open Tmds.Fuse
open FSFS
open System.IO

if not (Fuse.CheckDependencies()) then
    printfn "%s" Fuse.InstallationInstructions
else
    let fs = new HelloFPFileSystem() :> IFuseFileSystem // fsharplint:disable-line new indicates that type is IDisposable

    let mountPoint = "/tmp/HelloFS"

    printfn "Mounting HelloFilesystem at %s" mountPoint

    Fuse.LazyUnmount(mountPoint)
    Directory.CreateDirectory(mountPoint) |> ignore

    try
        using (Fuse.Mount(mountPoint, fs)) (fun mount ->
            printfn "Mount completed"
            mount.WaitForUnmountAsync() |> Async.AwaitTask |> Async.RunSynchronously)
    with :? FuseException as ex ->
        printfn "Fuse throw an exception: %A" ex
        printfn "Try unmounting the file system by executing:"
        printfn "fuser -kM %s" mountPoint
        printfn "sudo umount -f %s" mountPoint
