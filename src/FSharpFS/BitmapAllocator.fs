module FSFS.BitmapAllocator

open System.Threading
open System.Numerics

type Bitmap = uint64 array

let bitmapLen blockCount = (blockCount - 1) / 64 + 1

type private RotatedBitmap = uint * uint64 seq // start idx, Bitmap
let private startIdx (rotMask: RotatedBitmap) = fst rotMask
let private maskSeq (rotMask: RotatedBitmap) = snd rotMask

let rec private maskRotate pos (mask: Bitmap) =
    if pos >= 0 then
        RotatedBitmap(uint pos, mask |> Seq.permute (fun x -> (x + pos) % mask.Length))
    else
        maskRotate (mask.Length + pos) mask

type private BitmapAtomicOperation = uint * uint64 // Bitmap_index * mask

type private AllocateOp = BitmapAtomicOperation
type private FreeOp = BitmapAtomicOperation seq

let private setBitsIdxs num =
    // idxs starts from zero, 0 - MSB, 63 - LSB
    let rec setBitsIdxsHelper prevIdx num =
        seq {
            if num = 1UL then
                yield 63u
            else if num <> 0UL then
                let idx = BitOperations.LeadingZeroCount num |> uint
                yield (prevIdx + idx)
                yield! setBitsIdxsHelper (prevIdx + idx + 1u) (num <<< (int idx + 1))
        }

    setBitsIdxsHelper 0u num

let private blockBitmapIdx block = block / 64u
let private blockInMaskIdx block = block % 64u
let private blockIdx bitmaskIdx inMaskIdx = bitmaskIdx * 64u + inMaskIdx

let private blockMask (block: uint) =
    1UL <<< (64 - int (block |> blockInMaskIdx) - 1)

let private blocksMask blocks =
    blocks |> Seq.map blockMask |> Seq.reduce (|||)

let private allocateOperation num (rotMask: RotatedBitmap) =
    maskSeq rotMask
    |> Seq.mapi (fun bitmaskIdx mask -> uint bitmaskIdx, mask)
    |> Seq.skipWhile (fun (_, mask) -> ~~~mask = 0UL)
    |> Seq.tryHead
    |> Option.map (fun (bitmaskIdx, mask) ->
        let freeMask = ~~~mask
        let free = BitOperations.PopCount(freeMask) |> uint

        if num >= free then
            free, AllocateOp(bitmaskIdx, freeMask)
        else
            let lastIdx = setBitsIdxs freeMask |> Seq.item (num |> int) |> int
            num, AllocateOp(bitmaskIdx, freeMask >>> (64 - lastIdx) <<< (64 - lastIdx)))

let private freeOperation blocks : FreeOp =
    blocks
    |> Seq.sort
    |> Seq.groupBy blockBitmapIdx
    |> Seq.map (fun (bitmaskIdx, blocks) -> BitmapAtomicOperation(bitmaskIdx, blocksMask blocks))

exception DoubleFreeException of uint seq

let private emptyState blocksNum =
    Array.append
        (Array.init (bitmapLen (int blocksNum) - 1) (fun _ -> 0UL))
        [| (0UL ||| ((1UL <<< (64 - int blocksNum % 64)) - 1UL)) |]

type BitmapAllocator private (state: Bitmap, blocksNum: uint, initiallyAllocated: int) =
    let allocated = ref initiallyAllocated

    let randomGenerator = System.Random()

    let rec tryReserve n =
        let allocatedNow = allocated.Value

        if allocatedNow + int n <= int blocksNum then
            if allocatedNow = Interlocked.CompareExchange(allocated, allocatedNow + int n, allocatedNow) then
                true
            else
                tryReserve n
        else
            false

    let tryApply (op: AllocateOp) =
        let bitmaskIdx, mask = op
        (Interlocked.Or(&state[int bitmaskIdx], mask) &&& mask) = 0UL

    public new(state: Bitmap, blocksNum: uint) =
        let allocated =
            (state |> Seq.map BitOperations.PopCount |> Seq.reduce (+))
            - (64 - (int blocksNum) % 64)

        if (bitmapLen (int blocksNum)) <> state.Length then
            raise (System.ArgumentException())

        BitmapAllocator(state, blocksNum, allocated)

    public new(blocksNum: uint) = BitmapAllocator(emptyState blocksNum, blocksNum, 0)

    member this.Allocate(num) =
        if not <| tryReserve num then
            None
        else
            // It's guaranteed that allocator has enough free space
            let rec allocate num pos =
                let rotatedMask = state |> maskRotate -pos

                let allocated, rotatedOp = rotatedMask |> allocateOperation num |> Option.get

                let rotatedBitmapIdx, opMask = rotatedOp

                let bitmaskIdx =
                    (state.Length + int rotatedBitmapIdx - (startIdx rotatedMask |> int)) % state.Length

                let op = uint bitmaskIdx, opMask

                if tryApply op then
                    let blocks =
                        setBitsIdxs opMask |> Seq.map (blockIdx (uint bitmaskIdx)) |> List.ofSeq

                    if num = allocated then
                        blocks
                    else
                        blocks @ allocate (num - allocated) bitmaskIdx
                else
                    allocate num bitmaskIdx

            let res = allocate num (randomGenerator.Next(state.Length))
            Some res

    member this.Free(blocks) =
        let applyFreeOp (op: BitmapAtomicOperation) =
            let bitmaskIdx, opMask = op

            let doubleFreed =
                (Interlocked.And(&state[int bitmaskIdx], ~~~opMask) &&& opMask) ^^^ opMask

            if doubleFreed <> 0UL then
                raise (DoubleFreeException <| setBitsIdxs doubleFreed)

            Interlocked.Add(allocated, -BitOperations.PopCount(opMask)) |> ignore

        freeOperation blocks |> Seq.iter applyFreeOp

    member this.Available() = int blocksNum - allocated.Value

    member this.AllocatedBlocks() =
        state
        |> Seq.mapi (fun idx x -> idx, x)
        |> Seq.collect (fun (bitmaskIdx, mask) -> setBitsIdxs mask |> Seq.map (blockIdx (uint bitmaskIdx)))
        |> Seq.filter (fun block -> block < blocksNum)


    member this.State() = state

    member this.BlockMask(block) = blockBitmapIdx block, blockMask block
