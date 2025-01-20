module FSFS.FileSystemPersistence

open System.IO
open FsToolkit.ErrorHandling
open Tmds.Linux

open FSFS.Persistence
open FSFS.FileTree
open FSFS.FileTreePersistence
open FSFS.BitmapAllocator
open FSFS.FileSystem
open FSFS.BitmapAllocatorPersistence

let FSFSMagic = "F#FS"

let skipBlockStorageSize (info: BlockStorageInfo) (stream: Stream) =
    let areaSize = info.BlockSize * info.BlockCount
    stream.Position <- int64 areaSize
    stream

let tryReadObjectBlockStorage (stream: Stream) (filename: string) =
    option {
        let! info = tryDeserialize<BlockStorageInfo> stream

        let allocator = deserializeAllocator info.BlockCount filename stream

        let objectIO =
            new ObjectBlockStorageIOAdapterPool(filename, info.BlockSize, stream.Position |> uint32)

        stream |> skipBlockStorageSize info |> ignore

        return
            { Info = info
              ObjectIO = objectIO
              PersistableAllocator = allocator }
    }

let tryReadDataBlockStorage (stream: Stream) (filename: string) =
    option {
        let! info = tryDeserialize<BlockStorageInfo> stream

        let allocator = deserializeAllocator info.BlockCount filename stream

        let dataIO =
            new DataBlockStorageIOAdapterPool(filename, info.BlockSize, stream.Position |> uint32)

        stream |> skipBlockStorageSize info |> ignore

        return
            { Info = info
              DataIO = dataIO
              PersistableAllocator = allocator }
    }

let parseFileTree (mdStorage: ObjectBlockStorage) (stringStorage: ObjectBlockStorage) =
    let dtoSeq =
        mdStorage.PersistableAllocator.Allocator.AllocatedBlocks()
        |> Seq.map (fun x -> { BlockId = uint x })
        |> Seq.map (fun addr -> addr, mdStorage.ObjectIO.ReadObject<MetadataDTOBlock> addr)

    let strings =
        stringStorage.PersistableAllocator.Allocator.AllocatedBlocks()
        |> Seq.map (fun x -> { BlockId = uint x })
        |> Seq.map (fun addr -> addr, stringStorage.ObjectIO.ReadObject<string> addr)

    parseDTOBlocks dtoSeq strings

let tryReadFileSystem (filename: string) =
    option {
        use stream =
            File.Open(filename, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite)

        let! magic = tryDeserialize<string> stream

        let! _ = if magic = FSFSMagic then Some true else None

        let! mdStorage = tryReadObjectBlockStorage stream filename
        let! stringStorage = tryReadObjectBlockStorage stream filename
        let! dataStorage = tryReadDataBlockStorage stream filename

        let fileTree = parseFileTree mdStorage stringStorage

        return
            { FileTree = fileTree
              MetadataStorage = mdStorage
              StringStorage = stringStorage
              DataStorage = dataStorage }
    }

let createRootNode rootNameBlock rootBlockAddr =
    { Common =
        { Name = rootNameBlock, ""
          Addr = rootBlockAddr
          Uid = 0u
          Gid = 0u
          Mode = (0b111_111_111us |> mode_t.op_Implicit) // rwxr-xr-x
        }
      Content = Map.empty }

let formatStream mdBlocks stringBlocks dataBlocks (stream: Stream) =
    let mdAlloc = BitmapAllocator(mdBlocks)

    let rootMDBlock =
        { BlockId = mdAlloc.Allocate(1u) |> Option.get |> List.exactlyOne |> uint32 }

    let mdStorageInfo =
        { BlockSize = uint32 bigMetadataSize
          BlockCount = mdBlocks }

    let strAlloc = BitmapAllocator(stringBlocks)

    let rootNameBlock =
        { BlockId = strAlloc.Allocate(1u) |> Option.get |> List.exactlyOne |> uint32 }

    let strStorageInfo =
        { BlockSize = uint32 maxStrSize
          BlockCount = stringBlocks }

    let dataAllocator = BitmapAllocator(dataBlocks)

    let dataStorageInfo =
        { BlockSize = 4096u
          BlockCount = dataBlocks }

    stream
    |> serialize FSFSMagic
    |> serialize mdStorageInfo
    |> serializeState (mdAlloc.State())
    |> ignore

    let mdObjectIO =
        ObjectBlockStorageIOAdapter(stream, mdStorageInfo.BlockSize, stream.Position |> uint32)

    stream
    |> skipBlockStorageSize mdStorageInfo
    |> serialize strStorageInfo
    |> serializeState (strAlloc.State())
    |> ignore

    let strObjectIO =
        ObjectBlockStorageIOAdapter(stream, strStorageInfo.BlockSize, stream.Position |> uint32)

    stream
    |> skipBlockStorageSize strStorageInfo
    |> serialize dataStorageInfo
    |> serializeState (dataAllocator.State())
    |> skipBlockStorageSize dataStorageInfo
    |> ignore

    strObjectIO.WriteObject "" rootNameBlock

    let rootDirMD =
        createRootNode rootNameBlock rootMDBlock
        |> directoryMetadataToDTO rootParentAddr

    mdObjectIO.WriteObject rootDirMD rootMDBlock

    stream
