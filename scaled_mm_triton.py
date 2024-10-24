import torch
import triton
import triton.language as tl

from typing import Optional, Type


def prepare_matrix_for_triton(x: torch.Tensor):
    strides = x.stride()
    sizes = x.shape
    is_not_transpose = strides[0] == 1 and (strides[1] >= max(1, sizes[0]))
    is_transpose = strides[1] == 1 and (strides[0] >= max(1, sizes[1]))
    if not is_not_transpose and not is_transpose:
        return torch.clone(x, memory_format=torch.contiguous_format)
    return x


@triton.jit
def scaled_mm_kernel(a_ptr, b_ptr, scale_a_ptr, scale_b_ptr, c_ptr, bias_ptr,
                     M, N, K, stride_am, stride_ak, stride_bk, stride_bn,
                     stride_cm, stride_cn, BLOCK_SIZE_M: tl.constexpr,
                     BLOCK_SIZE_N: tl.constexpr, BLOCK_SIZE_K: tl.constexpr):
    pid = tl.program_id(axis=0)

    num_pid_n = tl.cdiv(N, BLOCK_SIZE_N)

    pid_m = pid // num_pid_n
    pid_n = pid % num_pid_n

    accumulator_dtype = tl.int32
    accumulator = tl.zeros((BLOCK_SIZE_M, BLOCK_SIZE_N),
                           dtype=accumulator_dtype)

    # NOTE: Some tensor inputs are so large, they will cause int32 overflow
    # so it is necessary to use tl.int64 for all the offsets, else SEGV will
    # eventually occur.

    # Offsets and masks.
    offsets_am = pid_m * BLOCK_SIZE_M + tl.arange(0, BLOCK_SIZE_M).to(tl.int64)
    masks_am = offsets_am < M

    offsets_bn = pid_n * BLOCK_SIZE_N + tl.arange(0, BLOCK_SIZE_N).to(tl.int64)
    masks_bn = offsets_bn < N

    offsets_k = tl.arange(0, BLOCK_SIZE_K).to(tl.int64)
    offsets_a = (stride_am * offsets_am[:, None] +
                 stride_ak * offsets_k[None, :])
    offsets_b = (stride_bk * offsets_k[:, None] +
                 stride_bn * offsets_bn[None, :])

    offsets_scale_a = offsets_am[:, None] + tl.arange(0, 1)[None, :].to(
        tl.int64)
    offsets_scale_b = offsets_bn[:, None] + tl.arange(0, 1)[None, :].to(
        tl.int64)

    a_ptrs = a_ptr + offsets_a
    b_ptrs = b_ptr + offsets_b

    scale_a_ptrs = scale_a_ptr + offsets_scale_a
    scale_b_ptrs = scale_b_ptr + offsets_scale_b

    for k in range(0, tl.cdiv(K, BLOCK_SIZE_K)):
        masks_k = offsets_k < K
        masks_a = masks_am[:, None] & masks_k[None, :]
        a = tl.load(a_ptrs, mask=masks_a)

        masks_b = masks_k[:, None] & masks_bn[None, :]
        b = tl.load(b_ptrs, mask=masks_b)

        # Accumulate results.
        accumulator = tl.dot(a, b, accumulator, out_dtype=accumulator_dtype)

        offsets_k += BLOCK_SIZE_K
        a_ptrs += BLOCK_SIZE_K * stride_ak
        b_ptrs += BLOCK_SIZE_K * stride_bk

    # Apply scale at end.
    masks_scale_a = masks_am[:, None] & (tl.arange(0, 1) < 1)[:, None]
    scale_a = tl.load(scale_a_ptrs, masks_scale_a)
    accumulator = scale_a * accumulator.to(tl.float32)

    masks_scale_b = masks_bn[:, None] & (tl.arange(0, 1) < 1)[None, :]
    scale_b = tl.load(scale_b_ptrs, masks_scale_b)
    accumulator = scale_b.T * accumulator.to(tl.float32)

    # Convert to output format.
    c = accumulator.to(c_ptr.type.element_ty)

    # Add bias, it's already in output format, so add it after conversion.
    if bias_ptr:
        offsets_bias = offsets_bn
        bias_ptrs = bias_ptr + offsets_bias
        bias_mask = offsets_bias < N
        bias = tl.load(bias_ptrs, bias_mask)
        c += bias

    # Save output
    offs_cm = pid_m * BLOCK_SIZE_M + tl.arange(0, BLOCK_SIZE_M).to(tl.int64)
    offs_cn = pid_n * BLOCK_SIZE_N + tl.arange(0, BLOCK_SIZE_N).to(tl.int64)
    offs_cm = offs_cm.to(tl.int64)
    offs_cn = offs_cn.to(tl.int64)
    c_ptrs = (c_ptr + stride_cm * offs_cm[:, None] +
              stride_cn * offs_cn[None, :])
    c_mask = (offs_cm[:, None] < M) & (offs_cn[None, :] < N)

    tl.store(c_ptrs, c, mask=c_mask)


# input   - [M, K]
# weight - [K, N]
def scaled_mm_triton(input: torch.Tensor,
                     weight: torch.Tensor,
                     scale_a: torch.Tensor,
                     scale_b: torch.Tensor,
                     out_dtype: Type[torch.dtype],
                     bias: Optional[torch.Tensor] = None,
                     block_size_m: int = 32,
                     block_size_n: int = 32,
                     block_size_k: int = 32) -> torch.Tensor:
    M, K = input.shape
    N = weight.shape[1]

    assert N > 0 and K > 0 and M > 0
    assert weight.shape[0] == K

    grid = lambda META: (triton.cdiv(M, META['BLOCK_SIZE_M']) * triton.cdiv(
        N, META['BLOCK_SIZE_N']), )

    # dtype=torch.float32,
    # device=input.device)
    result = torch.empty((M, N), dtype=out_dtype, device=input.device)

    has_scalar = lambda x: x.shape[0] == 1 and x.shape[1] == 1

    if has_scalar(scale_a):
        scale_a = scale_a[0][0] * torch.ones(
            (M, 1), dtype=torch.float32, device=input.device)

    if has_scalar(scale_b):
        scale_b = scale_b[0][0] * torch.ones(
            (M, 1), dtype=torch.float32, device=input.device)

    input = prepare_matrix_for_triton(input)
    weight = prepare_matrix_for_triton(weight)

    # A = input, B = weight, C = result
    # A = M x K, B = K x N, C = M x N
    scaled_mm_kernel[grid](input,
                           weight,
                           scale_a,
                           scale_b,
                           result,
                           bias,
                           M,
                           N,
                           K,
                           input.stride(0),
                           input.stride(1),
                           weight.stride(0),
                           weight.stride(1),
                           result.stride(0),
                           result.stride(1),
                           BLOCK_SIZE_M=block_size_m,
                           BLOCK_SIZE_N=block_size_n,
                           BLOCK_SIZE_K=block_size_k)

    # result = result.sum(0, dtype=torch.float32)
    # result = result.to(torch.float32)

    # print(f"=================result.shape = {result.shape}")
    return result


# a.shape = torch.Size([1, 17920]), b.shape = torch.Size([17920, 5120]),scale_a.shape = torch.Size([1, 1]), scale_b.shape = torch.Size([5120, 1]),bias.shape = None
# a.shape = torch.Size([1, 5120]), b.shape = torch.Size([5120, 35840]),scale_a.shape = torch.Size([1, 1]), scale_b.shape = torch.Size([35840, 1]),bias.shape = None
# a.shape = torch.Size([1, 5120]), b.shape = torch.Size([5120, 5120]),scale_a.shape = torch.Size([1, 1]), scale_b.shape = torch.Size([5120, 1]),bias.shape = None
# a.shape = torch.Size([1, 5120]), b.shape = torch.Size([5120, 7680]),scale_a.shape = torch.Size([1, 1]), scale_b.shape = torch.Size([7680, 1]),bias.shape = None
# a.shape = torch.Size([131072, 17920]), b.shape = torch.Size([17920, 5120]),scale_a.shape = torch.Size([131072, 1]), scale_b.shape = torch.Size([5120, 1]),bias.shape = None
# a.shape = torch.Size([131072, 5120]), b.shape = torch.Size([5120, 35840]),scale_a.shape = torch.Size([131072, 1]), scale_b.shape = torch.Size([35840, 1]),bias.shape = None
# a.shape = torch.Size([131072, 5120]), b.shape = torch.Size([5120, 5120]),scale_a.shape = torch.Size([131072, 1]), scale_b.shape = torch.Size([5120, 1]),bias.shape = None
# a.shape = torch.Size([131072, 5120]), b.shape = torch.Size([5120, 7680]),scale_a.shape = torch.Size([131072, 1]), scale_b.shape = torch.Size([7680, 1]),bias.shape = None
# a.shape = torch.Size([15, 17920]), b.shape = torch.Size([17920, 5120]),scale_a.shape = torch.Size([15, 1]), scale_b.shape = torch.Size([5120, 1]),bias.shape = None
# a.shape = torch.Size([15, 5120]), b.shape = torch.Size([5120, 35840]),scale_a.shape = torch.Size([15, 1]), scale_b.shape = torch.Size([35840, 1]),bias.shape = None
# a.shape = torch.Size([15, 5120]), b.shape = torch.Size([5120, 5120]),scale_a.shape = torch.Size([15, 1]), scale_b.shape = torch.Size([5120, 1]),bias.shape = None

# a.shape = torch.Size([15, 5120]), b.shape = torch.Size([5120, 7680]),
# scale_a.shape = torch.Size([15, 1]),
# scale_b.shape = torch.Size([7680, 1]),bias.shape = None

# a.shape = torch.Size([1, 17920]), a.dtype = torch.int8,
# b.shape = torch.Size([17920, 5120]), b.dtye = torch.int8,
# scale_a.shape = torch.Size([1, 1]), scale_a.dtype = torch.float32,
# scale_b.shape = torch.Size([5120, 1]), scale_b.dtype = torch.float32,
# bias.shape = None,bias.dtype = None


def scaled_mm_torch(a: torch.Tensor,
                    b: torch.Tensor,
                    scale_a: torch.Tensor,
                    scale_b: torch.Tensor,
                    out_dtype: Type[torch.dtype],
                    bias: Optional[torch.Tensor] = None) -> torch.Tensor:
    out = torch.mm(a.to(torch.float32), b.to(torch.float32))
    out = scale_a * out
    out = scale_b.T * out
    out = out.to(out_dtype)
    if bias is not None:
        out = out + bias

    return out


def main():

    which_test_fn = 0

    out_dtype = torch.float16

    golden_functions = [
        lambda a, b, scale_a, scale_b, bias: scaled_mm_torch(
            a, b, scale_a, scale_b, out_dtype, bias)
    ]
    golden_fn = golden_functions[which_test_fn]

    test_functions = [
        lambda a, b, scale_a, scale_b, bias: scaled_mm_triton(
            a, b, scale_a, scale_b, out_dtype, bias),
    ]
    test_fn = test_functions[which_test_fn]

    test_cases = [
        # M        K     N
        # Toy cases
        (32, 32, 32),
        (1, 17, 15),
        (15, 49, 19),
        (64, 96, 32),
        (27, 14, 103),
        (15, 179, 51),
        (15, 1792, 512),
        # Realistic cases
        (1, 17920, 5120),
        (1, 5120, 35840),
        (1, 5120, 5120),
        (1, 5120, 7680),
        (131072, 17920, 5120),
        (131072, 5120, 35840),
        (131072, 5120, 5120),
        (131072, 5120, 7680),
        (15, 17920, 5120),
        (15, 5120, 35840),
        (15, 5120, 5120),
        (15, 5120, 7680),
    ]

    use_bias = True

    use_scalar_scale_a = True
    use_scalar_scale_b = True

    comparisons = [torch.allclose]

    comparison = comparisons[which_test_fn]

    import time

    torch.manual_seed(0)

    test_out_dtype = torch.bfloat16

    for test_case in test_cases:
        M, K, N = test_case
        a = torch.randint(0, 127, (M, K), dtype=torch.int8, device='cuda')
        b = torch.randint(0, 127, (K, N), dtype=torch.int8, device='cuda')

        if use_scalar_scale_a:
            scale_a = torch.rand((1, 1), device='cuda')
        else:
            scale_a = torch.rand((M, 1), device='cuda')

        if use_scalar_scale_b:
            scale_b = torch.rand((1, 1), device='cuda')
        else:
            scale_b = torch.rand((1, 1), device='cuda')

        bias = None
        if use_bias:
            bias = torch.rand((N, ), device='cuda', dtype=out_dtype) * 10

        print("=" * 5 + f" Testing: mm_triton M={M}, K={K}, N={N}" + "=" * 5)

        # Compute and time test result.
        start = time.time()
        c_check = test_fn(a, b, scale_a, scale_b, bias)
        end = time.time()

        print(f"c_check time: {end - start}")
        print(f"c_check.dtype = {c_check.dtype}")

        a_cpu = a.cpu()
        b_cpu = b.cpu()
        scale_a_cpu = scale_a.cpu()
        scale_b_cpu = scale_b.cpu()
        bias_cpu = None if bias is None else bias.cpu()

        # Compute and time golden result.
        start = time.time()
        c_actual = golden_fn(a_cpu, b_cpu, scale_a_cpu, scale_b_cpu, bias_cpu)
        end = time.time()

        print(f"c_actual time: {end - start}")
        print(f"c_actual.dtype = {c_actual.dtype}")

        # Drrruuumrolll...
        comparison_result = comparison(c_check.cpu(),
                                       c_actual,
                                       rtol=1e-1,
                                       atol=1e-1)
        print(f"compare?: {comparison_result}")

        if not comparison_result:
            torch.set_printoptions(sci_mode=False)
            print(f"c_check = {c_check}")
            print(f"c_actual = {c_actual}")
            break


if __name__ == "__main__":
    main()
