const isoCountries = require('i18n-iso-countries');
isoCountries.registerLocale(require('i18n-iso-countries/langs/en.json'));

console.log('isoCountries.getName("IDN", "en"):', isoCountries.getName('IDN', 'en'));
console.log('isoCountries.getAlpha3Code("Indonesia", "en"):', isoCountries.getAlpha3Code('Indonesia', 'en'));

const testStr = 'Jakarta IDN';
const tokens = testStr.trim().split(/[,\s\-|;:]+/).filter(Boolean);
console.log('Tokens:', tokens);
const isValidIso3 = isoCountries.getName('IDN', 'en') !== undefined;
console.log('Is IDN a valid ISO3 code?:', isValidIso3);
