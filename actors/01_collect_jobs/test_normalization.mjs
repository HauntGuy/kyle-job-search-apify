import isoCountries from 'i18n-iso-countries';
import en from 'i18n-iso-countries/langs/en.json' assert { type: 'json' };

isoCountries.registerLocale(en);

console.log('isoCountries.getName("IDN", "en"):', isoCountries.getName('IDN', 'en'));
console.log('isoCountries.getAlpha3Code("Indonesia", "en"):', isoCountries.getAlpha3Code('Indonesia', 'en'));

const testStr = 'Jakarta IDN';
const tokens = testStr.trim().split(/[,\s\-|;:]+/).filter(Boolean);
console.log('\nTokens from "Jakarta IDN":', tokens);
const isValidIso3 = isoCountries.getName('IDN', 'en') !== undefined;
console.log('Is IDN a valid ISO3 code?:', isValidIso3);

console.log('\nIs IDN recognized as foreign?:', isoCountries.getName('IDN', 'en') && 'IDN' !== 'USA');
