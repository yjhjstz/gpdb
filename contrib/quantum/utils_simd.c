/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include "postgres.h"
#include <math.h>
#include <assert.h>

#ifdef __SSE__
#include <immintrin.h>
#endif

#ifdef __aarch64__
#include  <arm_neon.h>
#endif

#include  "hnsw.h"


/**************************************************
 * Get some stats about the system
 **************************************************/



#ifdef __AVX__
#define USE_AVX
#endif


/*********************************************************
 * Optimized distance computations
 *********************************************************/


/* Functions to compute:
   - L2 distance between 2 vectors
   - inner product between 2 vectors
   - L2 norm of a vector

   The functions should probably not be invoked when a large number of
   vectors are be processed in batch (in which case Matrix multiply
   is faster), but may be useful for comparing vectors isolated in
   memory.

   Works with any vectors of any dimension, even unaligned (in which
   case they are slower).

*/


/*********************************************************
 * Reference implementations
 */


float fvec_L2sqr_ref (const float * x,
                     const float * y,
                     size_t d)
{
    size_t i;
    float res = 0;
    for (i = 0; i < d; i++) {
        const float tmp = x[i] - y[i];
       res += tmp * tmp;
    }
    return res;
}

float fvec_L1_ref (const float * x,
                   const float * y,
                   size_t d)
{
    size_t i;
    float res = 0;
    for (i = 0; i < d; i++) {
        const float tmp = x[i] - y[i];
        res += fabs(tmp);
    }
    return res;
}

float fvec_Linf_ref (const float * x,
                     const float * y,
                     size_t d)
{
  size_t i;
  float res = 0;
  for (i = 0; i < d; i++) {
    res = fmax(res, fabs(x[i] - y[i]));
  }
  return res;
}

float fvec_inner_product_ref (const float * x,
                             const float * y,
                             size_t d)
{
    size_t i;
    float res = 0;
    for (i = 0; i < d; i++)
       res += x[i] * y[i];
    return res;
}

float fvec_norm_L2sqr_ref (const float *x, size_t d)
{
    size_t i;
    double res = 0;
    for (i = 0; i < d; i++)
       res += x[i] * x[i];
    return res;
}


void fvec_L2sqr_ny_ref (float * dis,
                    const float * x,
                    const float * y,
                    size_t d, size_t ny)
{
    for (size_t i = 0; i < ny; i++) {
        dis[i] = fvec_L2sqr (x, y, d);
        y += d;
    }
}




/*********************************************************
 * SSE and AVX implementations
 */

#ifdef __SSE__

// reads 0 <= d < 4 floats as __m128
static inline __m128 masked_read (int d, const float *x)
{
    assert (0 <= d && d < 4);
    __attribute__((__aligned__(16))) float buf[4] = {0, 0, 0, 0};
    switch (d) {
      case 3:
        buf[2] = x[2];
      case 2:
        buf[1] = x[1];
      case 1:
        buf[0] = x[0];
    }
    return _mm_load_ps (buf);
    // cannot use AVX2 _mm_mask_set1_epi32
}

float fvec_norm_L2sqr (const float *  x,
                      size_t d)
{
    __m128 mx;
    __m128 msum1 = _mm_setzero_ps();

    while (d >= 4) {
        mx = _mm_loadu_ps (x); x += 4;
        msum1 = _mm_add_ps (msum1, _mm_mul_ps (mx, mx));
        d -= 4;
    }

    mx = masked_read (d, x);
    msum1 = _mm_add_ps (msum1, _mm_mul_ps (mx, mx));

    msum1 = _mm_hadd_ps (msum1, msum1);
    msum1 = _mm_hadd_ps (msum1, msum1);
    return  _mm_cvtss_f32 (msum1);
}


#endif

#ifdef USE_AVX

// reads 0 <= d < 8 floats as __m256
static inline __m256 masked_read_8 (int d, const float *x)
{
    assert (0 <= d && d < 8);
    if (d < 4) {
        __m256 res = _mm256_setzero_ps ();
        res = _mm256_insertf128_ps (res, masked_read (d, x), 0);
        return res;
    } else {
        __m256 res = _mm256_setzero_ps ();
        res = _mm256_insertf128_ps (res, _mm_loadu_ps (x), 0);
        res = _mm256_insertf128_ps (res, masked_read (d - 4, x + 4), 1);
        return res;
    }
}

float fvec_inner_product (const float * x,
                          const float * y,
                          size_t d)
{
    __m256 msum1 = _mm256_setzero_ps();

    while (d >= 8) {
        __m256 mx = _mm256_loadu_ps (x); x += 8;
        __m256 my = _mm256_loadu_ps (y); y += 8;
        msum1 = _mm256_add_ps (msum1, _mm256_mul_ps (mx, my));
        d -= 8;
    }

    __m128 msum2 = _mm256_extractf128_ps(msum1, 1);
    msum2 +=       _mm256_extractf128_ps(msum1, 0);

    if (d >= 4) {
        __m128 mx = _mm_loadu_ps (x); x += 4;
        __m128 my = _mm_loadu_ps (y); y += 4;
        msum2 = _mm_add_ps (msum2, _mm_mul_ps (mx, my));
        d -= 4;
    }

    if (d > 0) {
        __m128 mx = masked_read (d, x);
        __m128 my = masked_read (d, y);
        msum2 = _mm_add_ps (msum2, _mm_mul_ps (mx, my));
    }

    msum2 = _mm_hadd_ps (msum2, msum2);
    msum2 = _mm_hadd_ps (msum2, msum2);
    return  _mm_cvtss_f32 (msum2);
}

float fvec_L2sqr (const float * x,
                 const float * y,
                 size_t d)
{
    __m256 msum1 = _mm256_setzero_ps();

    while (d >= 8) {
        __m256 mx = _mm256_loadu_ps (x); x += 8;
        __m256 my = _mm256_loadu_ps (y); y += 8;
        const __m256 a_m_b1 = mx - my;
        msum1 += a_m_b1 * a_m_b1;
        d -= 8;
    }

    __m128 msum2 = _mm256_extractf128_ps(msum1, 1);
    msum2 +=       _mm256_extractf128_ps(msum1, 0);

    if (d >= 4) {
        __m128 mx = _mm_loadu_ps (x); x += 4;
        __m128 my = _mm_loadu_ps (y); y += 4;
        const __m128 a_m_b1 = mx - my;
        msum2 += a_m_b1 * a_m_b1;
        d -= 4;
    }

    if (d > 0) {
        __m128 mx = masked_read (d, x);
        __m128 my = masked_read (d, y);
        __m128 a_m_b1 = mx - my;
        msum2 += a_m_b1 * a_m_b1;
    }

    msum2 = _mm_hadd_ps (msum2, msum2);
    msum2 = _mm_hadd_ps (msum2, msum2);
    return  _mm_cvtss_f32 (msum2);
}




#elif defined(__SSE__) // But not AVX

float fvec_L1 (const float * x, const float * y, size_t d)
{
    return fvec_L1_ref (x, y, d);
}

float fvec_Linf (const float * x, const float * y, size_t d)
{
    return fvec_Linf_ref (x, y, d);
}


float fvec_L2sqr (const float * x,
                 const float * y,
                 size_t d)
{
    __m128 msum1 = _mm_setzero_ps();

    while (d >= 4) {
        __m128 mx = _mm_loadu_ps (x); x += 4;
        __m128 my = _mm_loadu_ps (y); y += 4;
        const __m128 a_m_b1 = mx - my;
        msum1 += a_m_b1 * a_m_b1;
        d -= 4;
    }

    if (d > 0) {
        // add the last 1, 2 or 3 values
        __m128 mx = masked_read (d, x);
        __m128 my = masked_read (d, y);
        __m128 a_m_b1 = mx - my;
        msum1 += a_m_b1 * a_m_b1;
    }

    msum1 = _mm_hadd_ps (msum1, msum1);
    msum1 = _mm_hadd_ps (msum1, msum1);
    return  _mm_cvtss_f32 (msum1);
}


float fvec_inner_product (const float * x,
                         const float * y,
                         size_t d)
{
    __m128 mx, my;
    __m128 msum1 = _mm_setzero_ps();

    while (d >= 4) {
        mx = _mm_loadu_ps (x); x += 4;
        my = _mm_loadu_ps (y); y += 4;
        msum1 = _mm_add_ps (msum1, _mm_mul_ps (mx, my));
        d -= 4;
    }

    // add the last 1, 2, or 3 values
    mx = masked_read (d, x);
    my = masked_read (d, y);
    __m128 prod = _mm_mul_ps (mx, my);

    msum1 = _mm_add_ps (msum1, prod);

    msum1 = _mm_hadd_ps (msum1, msum1);
    msum1 = _mm_hadd_ps (msum1, msum1);
    return  _mm_cvtss_f32 (msum1);
}

#elif defined(__aarch64__)


float fvec_L2sqr (const float * x,
                  const float * y,
                  size_t d)
{
    if (d & 3) return fvec_L2sqr_ref (x, y, d);
    float32x4_t accu = vdupq_n_f32 (0);
    for (size_t i = 0; i < d; i += 4) {
        float32x4_t xi = vld1q_f32 (x + i);
        float32x4_t yi = vld1q_f32 (y + i);
        float32x4_t sq = vsubq_f32 (xi, yi);
        accu = vfmaq_f32 (accu, sq, sq);
    }
    float32x4_t a2 = vpaddq_f32 (accu, accu);
    return vdups_laneq_f32 (a2, 0) + vdups_laneq_f32 (a2, 1);
}

float fvec_inner_product (const float * x,
                          const float * y,
                          size_t d)
{
    if (d & 3) return fvec_inner_product_ref (x, y, d);
    float32x4_t accu = vdupq_n_f32 (0);
    for (size_t i = 0; i < d; i += 4) {
        float32x4_t xi = vld1q_f32 (x + i);
        float32x4_t yi = vld1q_f32 (y + i);
        accu = vfmaq_f32 (accu, xi, yi);
    }
    float32x4_t a2 = vpaddq_f32 (accu, accu);
    return vdups_laneq_f32 (a2, 0) + vdups_laneq_f32 (a2, 1);
}

float fvec_norm_L2sqr (const float *x, size_t d)
{
    if (d & 3) return fvec_norm_L2sqr_ref (x, d);
    float32x4_t accu = vdupq_n_f32 (0);
    for (size_t i = 0; i < d; i += 4) {
        float32x4_t xi = vld1q_f32 (x + i);
        accu = vfmaq_f32 (accu, xi, xi);
    }
    float32x4_t a2 = vpaddq_f32 (accu, accu);
    return vdups_laneq_f32 (a2, 0) + vdups_laneq_f32 (a2, 1);
}

// not optimized for ARM
void fvec_L2sqr_ny (float * dis, const float * x,
                        const float * y, size_t d, size_t ny) {
    fvec_L2sqr_ny_ref (dis, x, y, d, ny);
}

float fvec_L1 (const float * x, const float * y, size_t d)
{
    return fvec_L1_ref (x, y, d);
}

float fvec_Linf (const float * x, const float * y, size_t d)
{
    return fvec_Linf_ref (x, y, d);
}


#else
// scalar implementation

float fvec_L2sqr (const float * x,
                  const float * y,
                  size_t d)
{
    return fvec_L2sqr_ref (x, y, d);
}

float fvec_L1 (const float * x, const float * y, size_t d)
{
    return fvec_L1_ref (x, y, d);
}

float fvec_Linf (const float * x, const float * y, size_t d)
{
    return fvec_Linf_ref (x, y, d);
}

float fvec_inner_product (const float * x,
                             const float * y,
                             size_t d)
{
    return fvec_inner_product_ref (x, y, d);
}

float fvec_norm_L2sqr (const float *x, size_t d)
{
    return fvec_norm_L2sqr_ref (x, d);
}

void fvec_L2sqr_ny (float * dis, const float * x,
                        const float * y, size_t d, size_t ny) {
    fvec_L2sqr_ny_ref (dis, x, y, d, ny);
}


#endif