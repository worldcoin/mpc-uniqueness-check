use base64::prelude::*;

fn calculate_distance(code_a: Vec<u8>, mask_a: Vec<u8>, code_b: Vec<u8>, mask_b: Vec<u8>) -> f64 {
    let mut valid = 0;
    let mut diff = 0;

    for i in 0..code_a.len() {
        valid += (mask_a[i] & mask_b[i]).count_ones();
        diff += ((code_a[i] ^ code_b[i]) & (mask_a[i] & mask_b[i])).count_ones();
    }

    diff as f64 / valid as f64
}

#[cfg(test)]
mod tests {
    use crate::distance::{decode_distance, encode, Template};
    use super::*;

    #[test]
    fn test_calculate_distance_different_codes_hamming() {
        let code_a = BASE64_STANDARD.decode("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".as_bytes()).unwrap();
        let mask_a = BASE64_STANDARD.decode("////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////8wAAAAAAAAAAAAAAAD////////////////////////////////////////////////////////////////////////////////////////////////////////////////MzAAAAAAAAAAAAAAAAAAAAAAMz////////////////////////////////////////////////////////////////////////////////////////////////////////MzMAAAAAAAAAAAAAAAAAAAAAAAAAADMz///////////////////////////////////////////////////////////////////////////////////////////////////zMwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAzM///////////////////////////////////////////////////////////////////////////////////////////////MzMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAzM//////////////////////////////////////////////////////////////////////////////////////////zMzMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADMz/////////////////////////////////////////////8zMzMz///////////////////////////////////8zMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMzM///////////////////////////////////////8wAAAAAAAAAAAAA/////////////////////////////8zMwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAzMz/////////w==".as_bytes()).unwrap();
        let code_b = BASE64_STANDARD.decode("/s36gRd+zNuLmoKgKFd2Xd/97puKiTUgBXX//s25uogTKABXTfyaiaibqKADKiARUiABUzKJm6oDACZEXXdkRXX93/7om6qAACAyRXX23+zf3+ibqLqCAANXZXZF/f7sX0XXXXZFdkRf/s3N25uqKABXdt3/6KqKABEwdkXX/+7Nm7qKkyABN134iouqi6oCAyABUyKgAXdszfu6ABd0XlX3ZHXX/d/+qJuqggARNkX3f9/s3suom6i6igABF0X2Tf3/6JoBKXUgRXX1X/7N7N+boCBGRX3f/qiKAyAXZFdF/f/siJu6qJUgEyATqKmqqKugAgEgX3dmZFX/7N3/NmRXdXZHZFX1X//d/qqJuoKAE3ZFX9/f/KqKKF/tuqqgASJFdk39/6iKgSATAWR1d//d3+6JkyE2ZkfN37qoABF0XXRXXf3/6oiauqoDKBUgAyKJuqiSKAKBdFd3T83d/uzZU3ZkX3X2RkRXdd3/7NuqiboAABX3dF/f3f6bqgAXTfu6igAABXdd//uqqoAgExX2RXf+3f/ohX3/6KgCzbuqgAETZFX2X9/f39qLqoqCKCATKAKDKaioUgAAFXdXX1/N//7siTZmZFdXdkZFXX3d/+yZuomyAABXX37f7N3+2bqoAgF1/JuqAwXXXf7puqoAADJXdkXf/936qAXf36iKogG7qgABE2X1dkX+39/Km6qKgSACEiACiaqoKDKgAUX3ZFdEXf3f76k2RXX+RWRXZFX93f/siJuBUgFkX1/+z+zd/sm6qCKBdXXf/ooBB23/qLi6AgEyXXZFX/7fqqgRG7moiiABO6gAURUFRXRXdt/fyJu6iaEgAxKiiom6AiAyKgNlX2R2RFfs3/qoUEX1dmRFd2RFX93/7KibATZFXXff/smokTqJu6gyAXX1X/7LiKKJu6qKugKBF0X2Rd/+3+qom4uouiKAAZuoiyAyAhUgX0bf/qqJuomoqJu6ogABUgUhUjKDKTKBUkRXbd/oiCABXXZkXXdkZHXd3//smxEyRFdd3f6KiREyiZuhUgBXdX3+z6iKAZm6iJKCABdFdkzf5Fdkzf/JqLoyAgmbqKugUiATKTZF3/qiBX7buqibuiAAEzZH6JuokxUgETZGX1/t6JmyABNmZGXf7U/sdN//qoEzJldXXd36iAEX/oibKTKAXXd839+oioGpuqACEyRXzbqAEyRXZF//7eibshKJm6iagDEhKgEyATuDKAX33/qKm7ogABN2T+ibqBKTKFX0Xlff7Km7siATd2Rt3+yLqCABE2RFX2XXd/3duogCBfqKkyEyRXX3bN/d+KiBKTUgABdk3om6ABUgX2RX/+3sm7oSCbuomoEjASARKgE6gyAEXX/+yJu6oAATF02okwE2X23/qLiSATKJuxUgF3ZU7duoiyAAXXd1XXd13f/+ybqJKgEyCJUgAAX1X3zfzfqogDATKAATdf6JuoADKXZkXd/v7Lm6Eom7qLqJKgEgEyKBUwUgJkXf/sibqqAgExUgUhNFdl/d+om4kgAyATdXZFfU36m7KABFXXX3ZFXXds3d/om4myKAUkXfqogAKFX13936qKE026ioARE6SbqKASE2ZFXf7+z9up7N+6i6i6gAEyAyARdXZHZF3/7Um6ogERUZqDKDRX7f26iJugAwKAFXdk3/6JoAKkXXXf//6qBEX3ZN/d/puosiAAZF3+yKiSABN3df/szN/N+6qAARNk2rqgARU2RFX+zs/rqqATuoqJuJqpugEgARdkR2Xd/fzduqgBFkX/iagyH6iomooBKAUCAlX3bN/7iiAGRXX93/+qgBJFd2zf7fqLqLKgAWRX/+yougABKFX3bUzd3fuqigATUBKRuomTUgRXds/fqqKgFXZk3+zbqbqBKAKCKAXFdX3/3+qoATJFf+y4qJuooBUCAAEgFkdXX23f/6oAX3X13d37qiAWRXd+3fyai6iSAAEkRX//6LqKASAFX3X+393/qKogETAWXX7N37oAXX7P26iAKEXXZN/s26m6ACACACAgAXX1X//qqKUiRX/s+KibqAATEiABNFdXdkdF3//uRXdkbN3/+6KgX2RX/938i4mosgABKkXX/+iqiyAgAFd0ft/f/qiboBEw==".as_bytes()).unwrap();
        let mask_b = BASE64_STANDARD.decode("////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////8zAAAAAAAAAAAAAAAAAAAz////////////////////////////////////////////////////////////////////////////////////////////////////////////MzAAAAAAAAAAAAAAAAAAAAAAAAADM/////////////////////////////////////////////////////////////////////////////////////////////////////MzMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADMz///////////////////////////////////////////////////////////////////////////////////////////////zMzAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAzM///////////////////////////////////////////////////////////////////////////////////////////8zMwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMzP////////////////////////////////////////////8zP////////////////////////////////////////MzMwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP/////////////////////////////////////MAAAAAAAAAP///////////////////////////////////8zMzAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAzP//////w==".as_bytes()).unwrap();

        assert_eq!(calculate_distance(code_a, mask_a, code_b, mask_b), 0.5073757057002367);
    }

    #[test]
    fn test_calculate_distance_same_code_hamming() {
        let code_a = BASE64_STANDARD.decode("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".as_bytes()).unwrap();
        let mask_a = BASE64_STANDARD.decode("////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////8wAAAAAAAAAAAAAAAD////////////////////////////////////////////////////////////////////////////////////////////////////////////////MzAAAAAAAAAAAAAAAAAAAAAAMz////////////////////////////////////////////////////////////////////////////////////////////////////////MzMAAAAAAAAAAAAAAAAAAAAAAAAAADMz///////////////////////////////////////////////////////////////////////////////////////////////////zMwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAzM///////////////////////////////////////////////////////////////////////////////////////////////MzMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAzM//////////////////////////////////////////////////////////////////////////////////////////zMzMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADMz/////////////////////////////////////////////8zMzMz///////////////////////////////////8zMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMzM///////////////////////////////////////8wAAAAAAAAAAAAA/////////////////////////////8zMwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAzMz/////////w==".as_bytes()).unwrap();

        assert_eq!(calculate_distance(code_a.clone(), mask_a.clone(), code_a.clone(), code_a.clone()), 0.0);
    }

    #[test]
    fn test_calculate_distance_different_codes_hamming_mpc() {
        let template_a: Template = serde_json::from_str("{\"code\" :\"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\", \"mask\": \"////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////8wAAAAAAAAAAAAAAAD////////////////////////////////////////////////////////////////////////////////////////////////////////////////MzAAAAAAAAAAAAAAAAAAAAAAMz////////////////////////////////////////////////////////////////////////////////////////////////////////MzMAAAAAAAAAAAAAAAAAAAAAAAAAADMz///////////////////////////////////////////////////////////////////////////////////////////////////zMwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAzM///////////////////////////////////////////////////////////////////////////////////////////////MzMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAzM//////////////////////////////////////////////////////////////////////////////////////////zMzMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADMz/////////////////////////////////////////////8zMzMz///////////////////////////////////8zMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMzM///////////////////////////////////////8wAAAAAAAAAAAAA/////////////////////////////8zMwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAzMz/////////w==\"}").unwrap();
        let template_b: Template = serde_json::from_str("{\"code\" :\"/s36gRd+zNuLmoKgKFd2Xd/97puKiTUgBXX//s25uogTKABXTfyaiaibqKADKiARUiABUzKJm6oDACZEXXdkRXX93/7om6qAACAyRXX23+zf3+ibqLqCAANXZXZF/f7sX0XXXXZFdkRf/s3N25uqKABXdt3/6KqKABEwdkXX/+7Nm7qKkyABN134iouqi6oCAyABUyKgAXdszfu6ABd0XlX3ZHXX/d/+qJuqggARNkX3f9/s3suom6i6igABF0X2Tf3/6JoBKXUgRXX1X/7N7N+boCBGRX3f/qiKAyAXZFdF/f/siJu6qJUgEyATqKmqqKugAgEgX3dmZFX/7N3/NmRXdXZHZFX1X//d/qqJuoKAE3ZFX9/f/KqKKF/tuqqgASJFdk39/6iKgSATAWR1d//d3+6JkyE2ZkfN37qoABF0XXRXXf3/6oiauqoDKBUgAyKJuqiSKAKBdFd3T83d/uzZU3ZkX3X2RkRXdd3/7NuqiboAABX3dF/f3f6bqgAXTfu6igAABXdd//uqqoAgExX2RXf+3f/ohX3/6KgCzbuqgAETZFX2X9/f39qLqoqCKCATKAKDKaioUgAAFXdXX1/N//7siTZmZFdXdkZFXX3d/+yZuomyAABXX37f7N3+2bqoAgF1/JuqAwXXXf7puqoAADJXdkXf/936qAXf36iKogG7qgABE2X1dkX+39/Km6qKgSACEiACiaqoKDKgAUX3ZFdEXf3f76k2RXX+RWRXZFX93f/siJuBUgFkX1/+z+zd/sm6qCKBdXXf/ooBB23/qLi6AgEyXXZFX/7fqqgRG7moiiABO6gAURUFRXRXdt/fyJu6iaEgAxKiiom6AiAyKgNlX2R2RFfs3/qoUEX1dmRFd2RFX93/7KibATZFXXff/smokTqJu6gyAXX1X/7LiKKJu6qKugKBF0X2Rd/+3+qom4uouiKAAZuoiyAyAhUgX0bf/qqJuomoqJu6ogABUgUhUjKDKTKBUkRXbd/oiCABXXZkXXdkZHXd3//smxEyRFdd3f6KiREyiZuhUgBXdX3+z6iKAZm6iJKCABdFdkzf5Fdkzf/JqLoyAgmbqKugUiATKTZF3/qiBX7buqibuiAAEzZH6JuokxUgETZGX1/t6JmyABNmZGXf7U/sdN//qoEzJldXXd36iAEX/oibKTKAXXd839+oioGpuqACEyRXzbqAEyRXZF//7eibshKJm6iagDEhKgEyATuDKAX33/qKm7ogABN2T+ibqBKTKFX0Xlff7Km7siATd2Rt3+yLqCABE2RFX2XXd/3duogCBfqKkyEyRXX3bN/d+KiBKTUgABdk3om6ABUgX2RX/+3sm7oSCbuomoEjASARKgE6gyAEXX/+yJu6oAATF02okwE2X23/qLiSATKJuxUgF3ZU7duoiyAAXXd1XXd13f/+ybqJKgEyCJUgAAX1X3zfzfqogDATKAATdf6JuoADKXZkXd/v7Lm6Eom7qLqJKgEgEyKBUwUgJkXf/sibqqAgExUgUhNFdl/d+om4kgAyATdXZFfU36m7KABFXXX3ZFXXds3d/om4myKAUkXfqogAKFX13936qKE026ioARE6SbqKASE2ZFXf7+z9up7N+6i6i6gAEyAyARdXZHZF3/7Um6ogERUZqDKDRX7f26iJugAwKAFXdk3/6JoAKkXXXf//6qBEX3ZN/d/puosiAAZF3+yKiSABN3df/szN/N+6qAARNk2rqgARU2RFX+zs/rqqATuoqJuJqpugEgARdkR2Xd/fzduqgBFkX/iagyH6iomooBKAUCAlX3bN/7iiAGRXX93/+qgBJFd2zf7fqLqLKgAWRX/+yougABKFX3bUzd3fuqigATUBKRuomTUgRXds/fqqKgFXZk3+zbqbqBKAKCKAXFdX3/3+qoATJFf+y4qJuooBUCAAEgFkdXX23f/6oAX3X13d37qiAWRXd+3fyai6iSAAEkRX//6LqKASAFX3X+393/qKogETAWXX7N37oAXX7P26iAKEXXZN/s26m6ACACACAgAXX1X//qqKUiRX/s+KibqAATEiABNFdXdkdF3//uRXdkbN3/+6KgX2RX/938i4mosgABKkXX/+iqiyAgAFd0ft/f/qiboBEw==\", \"mask\": \"////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////8zAAAAAAAAAAAAAAAAAAAz////////////////////////////////////////////////////////////////////////////////////////////////////////////MzAAAAAAAAAAAAAAAAAAAAAAAAADM/////////////////////////////////////////////////////////////////////////////////////////////////////MzMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADMz///////////////////////////////////////////////////////////////////////////////////////////////zMzAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAzM///////////////////////////////////////////////////////////////////////////////////////////8zMwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMzP////////////////////////////////////////////8zP////////////////////////////////////////MzMwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP/////////////////////////////////////MAAAAAAAAAP///////////////////////////////////8zMzAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAzP//////w==\"}").unwrap();

        let encoded_template_a = encode(&template_a);
        let shares = encode(&template_b).share(2);

        let denominator = template_a.mask.dot(&template_b.mask);

        let share1_result = encoded_template_a.dot(&shares[0]);
        let share2_result = encoded_template_a.dot(&shares[1]);

        let numerator: u16 = share1_result.wrapping_add(share2_result);
        let numerators = [numerator; 31];
        let denominator = [denominator; 31];

        assert_eq!(decode_distance(&numerators, &denominator), 0.5073757057002367);
    }

    #[test]
    fn test_calculate_distance_same_code_hamming_mpc() {
        let template_a: Template = serde_json::from_str("{\"code\" :\"/szf3836AXZFX/7f/bqKgAKCRXX9/f7U3f/umoEyBF3bqLqKu6moihUiABUCRXf83oiJuoASKBfbqoATUkdF/s26qJsBUgFkXXX87N//7UX+ze2aioEgEyBFdXft//7BNFZXZXfs387N/kX2R3fd3+m6iqACAFdFdX/f/f3f7LqLuiABG6m6iLqLqKKBEyADKkX3Td/KqJqCAiBXX/7KmzKERX3f6LiaADKBZFX/7Ozf/8zf/smomoqAKBUgASX2ATKBKWRWX2Td/N/UxXZXdkdszf+ouougAgBXRXX//93+3fq6mpsiAAE1fqi6myKoiJuqiDKBNkX/zaiKggAgXXffzNu6KAZFfsy6mqATJWRX/+zEXXfN3/6JqKqKEyACAgEhUgEyASF0X1dkzfyaACR2X0dN/s3/qKqTKANXX0XX//7f//y6iLqTUiABdXd9/lZHRf3fqoiKKgKBds2omwKBJFX3383aiyACRXbU/pqokwUkXX/mRFX3z83fibqKABUyCoqbiiKhUgEgdkdXTU34uBKgX2ZF/N/PqaqDEyAXdFdFXf/+3936iokyATKARUX//uRWX2X+3/qKi6KBATKBKBUiRXRX/t/bqoUgFlX2bNzf6KqSKAF1dmRf/+7U25m6iAABUqqKm4KAUBUgKBdFX87N/LiSKgdmXX3f/omqAxEgF2RXXf///t/duogTdkX2RkXd/tqoABdlfs38+JuqigEyASA0ZFX03/7fuqgBUBZXZGRc3+qKkiABdX7Nu7qKAambuqADATuoiJoCABUTKAEyRXfuzf24myKBdldt3f6JqKKRUAJEX13/7N7aiaKCX1dkXkZF3f/bqKATJXZF/+z/qaqJKgUhNkRXfN/827qAATKSR2RXbN/qiBUkT/m4iaASABUgG6ogExO7qJqiACATu6gTKAZXfs3duJsiAXRXRd3+6KiKATKiRFdX/+zbuoACBkX3ZFZHX/3d/6iKASE2Rf/s+4m6iaKCAXZFd93/7Ki6gCAyEgNkX2Td/szX7N+5qKAgEgUXZFdl7Pzbq6iaoCAyX/uomgEyRXZU3/qKoBUkX0Xd/PqJqBUwKEXXXf/N+rqgABdFd2z+zf/s3f7ouoEgAEX/qKiJuomiAgF2X3Xf/+qoupuouhKDBFdkXf7U3+zfu6iCABUiJERXRX383+uomyAgF1X/iboBUgX2TN3+ioATJFZF3/z6ibATUCAEX1dfzfq6oAAHXXd9/s3/7N3+zdupogARu6qKESCZoiKAXld9//6oqJibqLqDAgRXdk3+yLok3fuokgATKqgSAAF1df/+6JugKFd93om6ASAXdkbN//qKEyBWRf/9+oiom7KAKFdXX///uqAgBXXXff7N+qgBdG3f+qqAGbuqiBUgEyUgKFRXff7eqKiam6iSAwZFX3bN/6ioASXf6JKgX+qom6giAAEXduzboCBXfN6JugEgFXZGRf3+qLAwXkX//e6KiZuqACAXdl3/7LigKAdFd13/7fqoASR03/qqgAmbqoASABO6oCBXX33+36qKkhEgJkX3Rd/+7NuqoBUgX2RFfN/uzPuJqooAABUFX2REXX3+ybqAKDAXKkX1/+i4EiZFff/siombqgEgF3bN/qiJsiAHRXZN/+3oiCEldF//qajNm6qAEgRX/oggF0X9/tqqgTAkRXRXfs3f7Oybi6gTAgX2zfqDKkXXzf6aqqAAARUFRkX9/s+qiZmwEyAXdf/suJK2RX//6KiZuoqLKBUiCbqKATKkRkX+z83t6KEXZXbf783szduKoBKkXf+qoBNFff6aqgEgZkX23f7U3+3834uogAKBX9+6gSAkX0X+36iqgAAAEXZFdf7f7s+bqoKgEXR9/fiQNkXf+6gAFf6aiqgCKAEyBkX3ZFXN/suJoBABXXXX3//v7c3bi6ATJFXf+qATZf36miABZXZFfs3+7FdlfN/LiqgSKBG7qKgCKEdlfd/oiqiiAAEzdXR+z83P26qCKBF1dd/s1FbN3bqoAXX9moqoAgARdkXXd3Rf7f6LiaAQKRX1X93/z83s35uoATJEX/6om6m4m6KgBXX2Rf/N/ORXZH3f/oqqgiABuqigAgJFXX3+6KmooiABEXd0Xszd/Zuogg==\", \"mask\": \"////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////8wAAAAAAAAAAAAAAAD////////////////////////////////////////////////////////////////////////////////////////////////////////////////MzAAAAAAAAAAAAAAAAAAAAAAMz////////////////////////////////////////////////////////////////////////////////////////////////////////MzMAAAAAAAAAAAAAAAAAAAAAAAAAADMz///////////////////////////////////////////////////////////////////////////////////////////////////zMwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAzM///////////////////////////////////////////////////////////////////////////////////////////////MzMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAzM//////////////////////////////////////////////////////////////////////////////////////////zMzMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADMz/////////////////////////////////////////////8zMzMz///////////////////////////////////8zMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMzM///////////////////////////////////////8wAAAAAAAAAAAAA/////////////////////////////8zMwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAzMz/////////w==\"}").unwrap();
        let template_b: Template = serde_json::from_str("{\"code\" :\"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\", \"mask\": \"////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////8wAAAAAAAAAAAAAAAD////////////////////////////////////////////////////////////////////////////////////////////////////////////////MzAAAAAAAAAAAAAAAAAAAAAAMz////////////////////////////////////////////////////////////////////////////////////////////////////////MzMAAAAAAAAAAAAAAAAAAAAAAAAAADMz///////////////////////////////////////////////////////////////////////////////////////////////////zMwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAzM///////////////////////////////////////////////////////////////////////////////////////////////MzMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAzM//////////////////////////////////////////////////////////////////////////////////////////zMzMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADMz/////////////////////////////////////////////8zMzMz///////////////////////////////////8zMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMzM///////////////////////////////////////8wAAAAAAAAAAAAA/////////////////////////////8zMwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAzMz/////////w==\"}").unwrap();

        let encoded_template_a = encode(&template_a);
        let shares = encode(&template_b).share(2);

        let denominator = template_a.mask.dot(&template_b.mask);

        let share1_result = encoded_template_a.dot(&shares[0]);
        let share2_result = encoded_template_a.dot(&shares[1]);

        let numerator: u16 = share1_result.wrapping_add(share2_result);
        let numerators = [numerator; 31];
        let denominator = [denominator; 31];

        assert_eq!(decode_distance(&numerators, &denominator), 0.0);
    }
}
