module FSFS.Allocator

open System.Threading
open System.Numerics

type Bitmask = uint64 array

type RotatedBitmask = uint * uint64 seq // start idx, Bitmask
let startIdx (rotMask: RotatedBitmask) = fst rotMask
let maskSeq (rotMask: RotatedBitmask) = snd rotMask

let rec maskRotate pos (mask: Bitmask) =
    if pos >= 0 then
        RotatedBitmask(uint pos, mask |> Seq.permute (fun x -> (x + pos) % mask.Length))
    else
        maskRotate (mask.Length + pos) mask

type BitmaskAtomicOperation = uint * uint64 // Bitmask_index * mask

type AllocateOp = BitmaskAtomicOperation
type FreeOp = BitmaskAtomicOperation seq

let setBitsIdxs num =
    // idxs starts from zero, 0 - MSB, 63 - LSB
    let rec setBitsIdxsHelper prevIdx num =
        seq {
            if num = 1UL then
                yield 63
            else if num <> 0UL then
                let idx = BitOperations.LeadingZeroCount num
                yield (prevIdx + idx)
                yield! setBitsIdxsHelper (prevIdx + idx + 1) (num <<< (idx + 1))
        }

    setBitsIdxsHelper 0 num

let blockBitmaskIdx block = block / 64 |> uint
let blockInMaskIdx block = block % 64 |> uint
let blockIdx bitmaskIdx inMaskIdx = bitmaskIdx * 64 + inMaskIdx
let blockMask (inMaskIdx: uint) = 1UL <<< (64 - int inMaskIdx - 1)

let blocksMask blocks =
    blocks |> Seq.map (blockMask << blockInMaskIdx) |> Seq.reduce (|||)

let allocateOperation num (rotMask: RotatedBitmask) =
    maskSeq rotMask
    |> Seq.mapi (fun bitmaskIdx mask -> uint bitmaskIdx, mask)
    |> Seq.skipWhile (fun (_, mask) -> ~~~mask = 0UL)
    |> Seq.tryHead
    |> Option.map (fun (bitmaskIdx, mask) ->
        let freeMask = ~~~mask
        let free = BitOperations.PopCount(freeMask)

        if num >= free then
            free, AllocateOp(bitmaskIdx, freeMask)
        else
            let lastIdx = setBitsIdxs freeMask |> Seq.item num
            num, AllocateOp(bitmaskIdx, freeMask >>> (64 - lastIdx) <<< (64 - lastIdx)))

let freeOperation blocks : FreeOp =
    blocks
    |> Seq.sort
    |> Seq.groupBy blockBitmaskIdx
    |> Seq.map (fun (bitmaskIdx, blocks) -> BitmaskAtomicOperation(bitmaskIdx, blocksMask blocks))

exception DoubleFreeException of int seq

type BlockAllocator(blocksNum) =
    let allocated = ref 0

    let state: Bitmask =
        Array.append
            (Array.init ((blocksNum - 1) / 64) (fun _ -> 0UL))
            [| (0UL ||| ((1UL <<< (64 - blocksNum % 64)) - 1UL)) |]

    let randomGenerator = System.Random()

    let rec tryReserve n =
        let allocatedNow = allocated.Value

        if allocatedNow + n <= blocksNum then
            if allocatedNow = Interlocked.CompareExchange(allocated, allocatedNow + n, allocatedNow) then
                true
            else
                tryReserve n
        else
            false

    let tryApply (op: AllocateOp) =
        let bitmaskIdx, mask = op
        (Interlocked.Or(&state[int bitmaskIdx], mask) &&& mask) = 0UL

    member this.Allocate(num) =
        if not <| tryReserve num then
            None
        else
            // It's guaranteed that allocator has enough free space
            let rec allocate num pos =
                let rotatedMask = state |> maskRotate -pos

                let allocated, rotatedOp = rotatedMask |> allocateOperation num |> Option.get

                let rotatedBitmaskIdx, opMask = rotatedOp

                let bitmaskIdx =
                    (state.Length + int rotatedBitmaskIdx - (startIdx rotatedMask |> int)) % state.Length

                let op = uint bitmaskIdx, opMask

                if tryApply op then
                    let blocks = setBitsIdxs opMask |> Seq.map (blockIdx (int bitmaskIdx)) |> List.ofSeq

                    if num = allocated then
                        blocks
                    else
                        blocks @ allocate (num - allocated) bitmaskIdx
                else
                    allocate num bitmaskIdx

            let res = allocate num (randomGenerator.Next(state.Length))
            Some res

    member this.Free(blocks) =
        let applyFreeOp (op: BitmaskAtomicOperation) =
            let bitmaskIdx, opMask = op

            let doubleFreed =
                (Interlocked.And(&state[int bitmaskIdx], ~~~opMask) &&& opMask) ^^^ opMask

            if doubleFreed <> 0UL then
                raise (DoubleFreeException <| setBitsIdxs doubleFreed)

            Interlocked.Add(allocated, -BitOperations.PopCount(opMask)) |> ignore

        freeOperation blocks |> Seq.iter applyFreeOp

    member this.Available() = blocksNum - allocated.Value
