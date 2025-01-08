module FSFS.FileTreePersistence

open FSFS.Persistence
open FSFS.FileTree

open Tmds.Linux
open MBrace.FsPickler
open System.IO

let rootParentAddr: BlockStorageAddr = { BlockId = System.UInt32.MaxValue }

type CommonMetadataDTO =
    { Name: BlockStorageAddr
      Parent: BlockStorageAddr
      Uid: uint
      Gid: uint
      Mode: mode_t }

type FileMetadataDTO =
    { Common: CommonMetadataDTO
      Content: BlockStorageAddr array
      Extension: BlockStorageAddr option }

let maxBlocksInNode = 64

type FileMetadataExtensionDTO =
    { Content: BlockStorageAddr array
      Extension: BlockStorageAddr option }

type DirectoryMetadataDTO = { Common: CommonMetadataDTO }

type MetadataDTOBlock =
    | FileDTO of FileMetadataDTO
    | FileExtensionDTO of FileMetadataExtensionDTO
    | DirectoryDTO of DirectoryMetadataDTO

let commonMetadataToDTO (parent: BlockStorageAddr) (commonMD: CommonMetadata)=
    { Name = stringAddr commonMD.Name
      Parent = parent
      Uid = commonMD.Uid
      Gid = commonMD.Gid
      Mode = commonMD.Mode }

let directoryMetadataToDTO (parent: BlockStorageAddr) (dirMD: DirectoryMetadata)  =
    DirectoryDTO({ Common = commonMetadataToDTO parent dirMD.Common })

let fileMetadataToDTOs (fileMD: FileMetadata) (parent: BlockStorageAddr) =
    let contentSeq = fileMD.Content |> Seq.chunkBySize maxBlocksInNode

    let fileMDDTO: FileMetadataDTO =
        { Common = commonMetadataToDTO parent fileMD.Common 
          Content = contentSeq |> Seq.head 
          Extension = fileMD.ExtensionAddrs |> List.tryHead }

    if fileMD.ExtensionAddrs.IsEmpty then
        seq { FileDTO(fileMDDTO) }
    else
        let extAddrSeq =
            Seq.append (fileMD.ExtensionAddrs |> Seq.skip 1 |> Seq.map Some) (seq { None })

        Seq.append
            (seq { FileDTO(fileMDDTO) })
            (contentSeq
             |> Seq.skip 1
             |> Seq.zip extAddrSeq
             |> Seq.map (fun (addr, blocks) ->
                 FileExtensionDTO(
                     { FileMetadataExtensionDTO.Content = blocks 
                       FileMetadataExtensionDTO.Extension = addr }
                 )))

let nodeToDTOs (node: FileTreeNode) (parent: BlockStorageAddr) =
    match node with
    | FileNode(fileMD) -> fileMetadataToDTOs fileMD parent
    | DirectoryNode(dirMD) -> seq { directoryMetadataToDTO parent dirMD }
    | FillerNode(name) -> seq {}

let tryUnwrapFile block =
    match block with
    | FileDTO(fileMD) -> Some fileMD
    | _ -> None

let tryUnwrapFileExt block =
    match block with
    | FileExtensionDTO(fileExtMD) -> Some fileExtMD
    | _ -> None

let tryUnwrapDir block =
    match block with
    | DirectoryDTO(dirMD) -> Some dirMD
    | _ -> None

let private parseCommonMetadataDTO strMap (addr: BlockStorageAddr) (commonMDDTO: CommonMetadataDTO) =
    let name = strMap |> Map.find commonMDDTO.Name

    { Name = commonMDDTO.Name, name
      Addr = addr
      Uid = commonMDDTO.Uid
      Gid = commonMDDTO.Gid
      Mode = commonMDDTO.Mode }

let rec private parseFileExtension (fileExts: Map<BlockStorageAddr, FileMetadataExtensionDTO>) addr =
    let fileExtMDDTO = fileExts |> Map.find addr

    (addr, fileExtMDDTO.Content)
    :: (fileExtMDDTO.Extension
        |> Option.map (parseFileExtension fileExts)
        |> Option.defaultValue [])

let private parseFile strMap fileExts addr (fileMDDTO: FileMetadataDTO) =
    let commonMD = parseCommonMetadataDTO strMap addr fileMDDTO.Common

    let fileContent = fileMDDTO.Content
    let extensions = fileMDDTO.Extension |> Option.map (parseFileExtension fileExts)
    let extContents = extensions |> Option.map (List.map snd) |> Option.defaultValue []
    let extAddrs = extensions |> Option.map (List.map fst) |> Option.defaultValue []

    let content = ([ fileContent ] @ extContents) |> Seq.concat |> List.ofSeq

    FileNode
        { Common = commonMD
          Content = content
          ExtensionAddrs = extAddrs }

let parseDTOBlocks (dtoSeq: (BlockStorageAddr * MetadataDTOBlock) seq) (strings: StringWithAddr seq) =
    let seqTryUnwrapSecond tryUnwrap sequence =
        sequence
        |> Seq.choose (fun (f, s) -> s |> tryUnwrap |> Option.map (fun s -> f, s))

    let cachedDto = dtoSeq |> Seq.cache

    let dirs = cachedDto |> seqTryUnwrapSecond tryUnwrapDir
    let files = cachedDto |> seqTryUnwrapSecond tryUnwrapFile
    let fileExts = cachedDto |> seqTryUnwrapSecond tryUnwrapFileExt |> Map.ofSeq

    let strMap = strings |> Map.ofSeq

    let parentToDirs =
        dirs |> Seq.groupBy (fun (_, dirMDDTO) -> dirMDDTO.Common.Parent) |> Map.ofSeq
    
    let parentToFiles =
        files
        |> Seq.groupBy (fun (_, fileMDDTO) -> fileMDDTO.Common.Parent)
        |> Map.ofSeq

    let rec parseDirTree addr dirMDDTO =
        let commonMD = parseCommonMetadataDTO strMap addr dirMDDTO.Common

        let childrenDirs =
            parentToDirs
            |> Map.tryFind addr
            |> Option.defaultValue (seq{})
            |> Seq.map (fun (addr, childMDDTO) -> parseDirTree addr childMDDTO)

        let childrenFiles =
            parentToFiles
            |> Map.tryFind addr
            |> Option.defaultValue (seq{})
            |> Seq.map (fun (addr, childMDDTO) -> parseFile strMap fileExts addr childMDDTO)

        DirectoryNode
            { DirectoryMetadata.Common = commonMD
              DirectoryMetadata.Content = 
                Seq.append childrenDirs childrenFiles 
                |> Seq.map (fun node -> nodeName node, node) 
                |> Map.ofSeq 
            }

    let (rootDirAddr, rootDirMD) =
        parentToDirs |> Map.find rootParentAddr |> Seq.exactlyOne

    parseDirTree rootDirAddr rootDirMD

let private binarySerializer = FsPickler.CreateBinarySerializer()

let private commonMetadata =
    { Name = { BlockId = 10u }
      Parent = { BlockId = 10u }
      Uid = 10u
      Gid = 10u
      Mode = 0us |> mode_t.op_Implicit }

let commonMetadataSize = binarySerializer.ComputeSize commonMetadata

let private bigMetadata: MetadataDTOBlock =
    FileDTO(
        { Common = commonMetadata
          Content = Array.init maxBlocksInNode (fun x -> { BlockId = 0xDEADBEEFu })
          Extension = Some { BlockId = 10u } }
    )

let bigMetadataSize = binarySerializer.ComputeSize bigMetadata

let private maxStr = String.init 256 (fun _ -> "a")
let maxStrSize = binarySerializer.ComputeSize maxStr