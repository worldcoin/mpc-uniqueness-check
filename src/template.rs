use std::fmt::Debug;

use bytemuck::{Pod, Zeroable};
use itertools::izip;
use rand::distributions::{Distribution, Standard};
use rand::Rng;
use serde::{Deserialize, Serialize};

pub use crate::bits::Bits;

#[repr(C)]
#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Debug,
    Default,
    Serialize,
    Deserialize,
    Pod,
    Zeroable,
)]
pub struct Template {
    pub code: Bits,
    pub mask: Bits,
}

impl Template {
    pub fn rotations(&self) -> impl Iterator<Item = Template> + '_ {
        let codes = self.code.rotations();
        let masks = self.mask.rotations();
        codes
            .into_iter()
            .zip(masks)
            .map(|(code, mask)| Template { code, mask })
    }

    pub fn distance(&self, other: &Self) -> f64 {
        self.rotations()
            .map(|r| r.fraction_hamming(other))
            .fold(f64::INFINITY, |a, b| a.min(b))
    }

    pub fn fraction_hamming(&self, other: &Self) -> f64 {
        let mut num = 0;
        let mut den = 0;

        for (ap, am, bp, bm) in izip!(
            self.code.0.iter(),
            self.mask.0.iter(),
            other.code.0.iter(),
            other.mask.0.iter()
        ) {
            let m = am & bm;
            let p = (ap ^ bp) & m;
            num += p.count_ones();
            den += m.count_ones();
        }

        // tmp_dist
        (num as f64) / (den as f64)
    }
}

impl Distribution<Template> for Standard {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> Template {
        Template {
            code: rng.gen(),
            mask: rng.gen(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distance;

    const A: &str = indoc::indoc! { r#"{
        "code": "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",
        "mask":"//////////////////////z//////////////////////8zM///////////8z//M/////////////M/P/////P////MzMzAAADMwAP///////////////////////8zM////////////////////////////////////////////////////////////////////////////////////////////////MzMAAAAzMzP//8zP////////////////////////////////////////////////////////////////////////////////////////////////////////////////////8zMzAAAAAzMzMwAAAP////////////////////////////////////////////////////////////////////////////////////////////////////////////////////MzMwAAAAAzMzAAAAAzMz////////////////////////////////////////////////////////////////////////////////////////////////////////////////8zMzMAAAAAADAAAAAAAAAzMzMAAAAM////////////////////////////////////////////////////////////////////////////////////////////////////////MzMzAAAAAAAAAAAAAAAAAAAAAAAAAAADMz/////////////////////////////////////////////////////////////////////////////////////////////////zMzAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMzP///////////////////////////////////////////////////////////////////////////////////////////8zAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMzP///////////////////////////////////////////////////////////////////////////////////////8zMzAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMzP//////////////////////////////////////////////////////////////////////////////////////zMzMwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMzP/////////////////////////////////////////////////////////////////////////////////////MzMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMzP///////////////////////////////////////////////////////////////////////////////////MzMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADMzP/////////////////////////////////////////////////////////////////////////////////MzMwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAzM/////////////////////////////////////////////////////////////////////////////////8zMzAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMzM///////////////////////////////////////////////////////////////////////////////8zMzAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAzMz/////////////////////////////////////////////////////////////////////////////zMzMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAzMz///w=="
    }"# };

    const B: &str = indoc::indoc! { r#"{
        "code": "ibqIuouomyABMyAgG4mom4moIBMiITMiRXds3+32RXdk7N/+iJqgASBkVX7f+qiTASV2RX/+37qLIAF1fsm6iSAROom6IAF1dkVf//7N32RXTf7N/s3/zbogE2RV/f6ouqATMs26iJuKiZsgATIgATKJqJuJICASiiAzIkV3fN/ldlV3bM3f6qiaACATZkV+3/qIEwFkdkV//t36iyABdX7N+om6iACZMiABVXdkXf/+iJsgF0V83fqBds3boBdkX/36qLIAF17d+ombqomTIAEyIBMgAbqLibqIMgMgMldF/t6AF2RVd+zN/s26qoMgEkZFft/6gFdFdERVduzd+okiAXVs3/qIuqIAEzIiAFVXZN3++oiDIBNldX3+R2Xd3+ibIFftm6ggAVZN/7qJuyIDEyCJuqiTIBE2Tem6iBITBHZf/u6LqBMgF3dkRXbN//qboDIDdXTf9kV3XfzNd3bN37qIMgFX7N+qiooCABMxIgBFd2Td/LqIgyAzZFd1dldt393+iKoTIBu6IgF2Tbu6iCAyBVdk3+y4mqARMgNpu6gTAkV2zf/szd+6qLIAMEV2Rez926iyATJFV0V+zd3+iKB2zduogBNk3+yKiJqAAyARETJkRXds3f26iAIBMkVXdVdt3f/t/oibEyJFdt/tuom7m6oBMkVXZN/t2bqIkiATIRMgEwdF/siTIGRf/JuqACE1VXZERV/N+oEwRVdF/s39+oqgEhCbIAF1bN+qiIkyIBE0RFd3ZEVXft39uoiAATA2VXVXZN3+7f7MV0V2Td/fqLqBdX+iBXdHbezf7om6iboAEyATJFZN36iIkyBkV+zbqoABIFV0BkVXTd/s5EVX3f7N/bqbqAEwEgIBbfibqgAhF2RVd3Tf/+5FV/7M37qqoAMhMgAwV2RXfu32RVdFfs3f+6gSAWV2Vk3f/omooCIJuomoABMgEkRXzduoiRMgNFfs26qCASBVcAJFV0Vd/s3+zdu6AXXd26iZMBISiaibkyIBdFV1Vf/+3f3+zf3f7N/JuqiDITALqJMgAyAldkVXZX/N/4uoEgdldl383fqKqCACAbqJqJAyADZEX9+6iJETIDATIJugAkVd3boAJVZFXf7N/omZMgNX7duomqAwEgIgEwIkVXRVdlX93938/s/+3+zf6buoizKouoi6oAAgABF2RXV33f7JqAAXRX7d/sm6qKggAgE4maiIMgEhZE3/6oiam6iokgATIEZEX/26ASRXV13+zf6IiTIGR+3bqIqoshIiABZHdVd2VXdVdd/9/szf/s/u36i6qIm6ibqJuqACICIBMkV0VV3+zfqAF9/+ibqJu6iaIgIFbd24iLIDIXZF/P6om4uoqJIBICASRV/fqAF0VXdP7N/v7WRf/s/t24qKiLoCIgBXV3VX/1V3V1Xf3f7M3/7f7s/tuoioqom6ibqoAgAiAAIVd2RV/9/f7N/7mom6ARMgEyIBdk39+oigISR2Rez+qJupICASAyAgEgAgEyADZFV2R2Xf/83+3f7N7dqLqoiyAgJFV1d1V//9/3dN/s3+zd/s387P7bqIqIqoqom6qIIBIgAAAVV3V33f3+zfuJuokgEyAFV2RX7N/bqLIAEkVk/s36iboAAgMgE6iJqKoBMgARE2ZEdlVf7N/t/+zezfqJqoIiIFVVd3ZVX/3f/f7PZEV0VXZVX/3+36qKiZqKqJuqioiyAAIgABVXdk3//s37qbqIMgIgVVfu3+iam6gyABdFdF7N+IiaADIDJFfsmbqoiTIAARMiRXV0V/7f7f7s3826iboAAgVXdVd3/d3/36iqggABMkV0VVd93////ouqiqi4uoqJqgAAIiABFXdVX/7N36i6qAIAMFdd/ouqipMgEgBGRXZN3fi6mgEyACBXZPy7qIm6AgEyAkV1dFV/3/3+7N/9momiAABVV2VV//3d+qqoqgAAADIFdVdVVV39//zeiIioqLqKiqqAACIgABJFV1X/39/tupqDIBRXzbqJqoISIBMkZkV2zf3oupsBMgAgEyVk+qiIqAIBIgNVdXRVdd/9/N7f35upIgBkVXd1Xf/f2bqogAAAACAyBXVXVVVVXd/8/siZqoiKiqiqiIIiIAACBFd1///Q==",
        "mask":"////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////z////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////Mz//Mz///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////zAMwAAMwAAAAMzM///zMz/////////////////////////////////////////////////////////////////////////////////////////////////////////////MzMAAAAAAAAAAAAAAAAAAAAAAzM//////////////////////////////////////////////////////////////////////////////////////////////////////M8zMzAAAAAAAAAAAAAAAAAAAAAAAAAAADM////////////////////////////////////////////////////////////////////////////////////////////////zMwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADMzMADP////////////////////////////////////////////////////////////////////////////////////////8zMzAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAM///////////////////////////////////////////////////////////////////////////////////8zMzAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADM///////////////////////////////////////////////////////////////////////////////8zMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADMz////////////////////////////////////////////////////////////////////////////MzMwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMzP/////////////////////////////////////////////////////////////////////////MzAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADMz///////////////////////////////////////////////////////////////////////zMzAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADMz//////////////////////////////////////////////////////////////////////MzMwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADMz///////////////////////////////MzMzMz///////////////////////////////8zMzAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAzMzP/////////////////////////8zMAAAAAAAAAMz////////////////////////////MzMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMzMw=="
    }"# };

    #[test]
    fn hamming_distance_with_rotations() {
        let a: Template = serde_json::from_str(A).unwrap();
        let b: Template = serde_json::from_str(B).unwrap();

        let d = a.distance(&b);

        assert_eq!(d, 0.3352968352968353);
    }

    #[test]
    fn hamming_distance_no_rotations() {
        let a: Template = serde_json::from_str(A).unwrap();
        let b: Template = serde_json::from_str(B).unwrap();

        let d = a.fraction_hamming(&b);

        assert_eq!(d, 0.47833602212491355);
    }
}
