'use strict';

const { createReadStream, createWriteStream, writeFile } = require('fs');
const { AsyncParser } = require('json2csv');
const request = require('request-promise');

const translations = require('./translations');
const mapFields = require('./mapFields');
const sortedFields = require('./sorted_fields');

const adaptedTranslations = {};
for (const key of Object.keys(translations)) {
  adaptedTranslations[key.toLowerCase()] = translations[key];
}

function getFields() {
  // Fields that come in the export to CSV file.
  const fields = 'Name,secId,Symbol,Last Close Price,Price Currency,Yield(%),Ongoing Charge(%),Morningstar Category,Morningstar Analyst Rating™,Morningstar Rating™,Morningstar Sustainability Rating™,1 Day Return(%),1 Week Return(%),1 Month Return(%),3 Months Return(%),6 Months Return(%),YTD Return(%),1 Year Annualised(%),3 Years Annualised(%),5 Years Annualised(%),10 Years Annualised(%),Initial Charge(%),Manager Tenure(Yrs),Deferred Fee(%),Minimum Initial Purchase,Fund Size(Mil),Equity Style Box™,Fixed Income Style Box™,Average Market Cap(Mil),Average Credit Quality,Effective Duration,Morningstar Risk (Rel to Category),3 Year Alpha,3 Year Beta,3 Year R-Squared,3 Year Standard Deviation,3 Year Sharpe Ratio'
    .split(',');
  const fieldsForUrl = new Set();
  for (const [key, value] of Object.entries(translations)) {
    if (!value.en) {
      continue;
    }
    if (fields.includes(value.en)) {
      if (!mapFields[key]) {
        console.log('Error getting key: ', key);
        continue;
      }
      fieldsForUrl.add(mapFields[key]);
    }
  }

  // Here are the fields that come by default.
  const otherFields = 'SecId|Name|PriceCurrency|TenforeId|LegalName|Ticker|ClosePrice|OngoingCharge|CategoryName|AnalystRatingScale|StarRatingM255|SustainabilityRank|GBRReturnD1|GBRReturnW1|GBRReturnM1|GBRReturnM3|GBRReturnM6|GBRReturnM0|GBRReturnM12|GBRReturnM36|GBRReturnM60|GBRReturnM120|MaxFrontEndLoad|ManagerTenure|MaxDeferredLoad|InitialPurchase|FundTNAV|EquityStyleBox|BondStyleBox|AverageMarketCapital|AverageCreditQualityCode|EffectiveDuration|MorningstarRiskM255|AlphaM36|BetaM36|R2M36|StandardDeviationM36|SharpeM36|TrackRecordExtension'
    .split('|');

  // Fields for Damien
  const extraFields = 'ExpenseRatio|ExpenseRatioGross|ManagementExpenseRatio|DebtEquityRatio|DividendYield|EBTMarginYear1|FairValueEstimate|Average12MonthFossilFuelExposure|fundTNAV|PERatio5YrAverage|PEGRatio|PERatio|TotalAssets'
    .split('|');

  return [...new Set([...fieldsForUrl, ...otherFields, ...extraFields])];
}

async function getBrandingCompanyIds() {
  const data = await request(
    'https://lt.morningstar.com/api/rest.svc/klr5zyak8x/security/screener?currencyId=CAD&universeIds=ETCAN%24%24FFE%7CETUSA%24%24FFE&outputType=json&filterDataPoints=BrandingCompanyId',
    {
      json: true,
    }
  );
  // return data.filters[0][0].BrandingCompanyId.map(e => e.id).slice(0, 3);
  return data.filters[0][0].BrandingCompanyId.map(e => e.id);
}

async function getDataForBrandingCompanyId(brandingCompanyId, fields) {
  const url = 'https://lt.morningstar.com/api/rest.svc/klr5zyak8x/security/screener?page=1&pageSize=100000&sortOrder=LegalName%20asc&outputType=json&version=1&currencyId=CAD&universeIds=ETCAN$$FFE|ETUSA$$FFE&' +
    `securityDataPoints=${fields.join('|')}` +
    `&filters=BrandingCompanyId:IN:${brandingCompanyId}&term=&subUniverseId=`;

  let data;
  try {
    data = await request(
      url,
      {
        json: true,
      }
    );
  } catch (e) {
    console.log('e', e);
  }
  return data.rows;
}

(async () => {
  const brandingCompanyIds = await getBrandingCompanyIds();

  const rawFields = getFields();

  const data = (await Promise.all(brandingCompanyIds.map(
    (brandingCompanyId) => getDataForBrandingCompanyId(brandingCompanyId, rawFields))
  )).flat();

  // Convert translations to lowerCase
  const fields = [...rawFields].map((field) => {
    return {
      label: adaptedTranslations[field.toLowerCase()]?.en || field,
      value: field,
    }
  });

  let outputFields = [];
  // Try sorting the fields:
  for (const sortedField of sortedFields) {
    const field = fields.find(s => (s.value === sortedField || s.label === sortedField));

    if (field) {
      outputFields.push(field);
    }
  }

  console.log(`Had ${fields.length} initial fields. Now has ${outputFields.length} sorted fields.`);

  const opts = { fields: outputFields };
  const transformOpts = { highWaterMark: 8192 };

  const output = createWriteStream('output.csv', { encoding: 'utf8' });
  const asyncParser = new AsyncParser(opts, transformOpts);
  const parsingProcessor = asyncParser.toOutput(output);

  parsingProcessor.input.push(JSON.stringify(data));
  parsingProcessor.input.push(null);

  console.log('Data written to output.csv');
})();
