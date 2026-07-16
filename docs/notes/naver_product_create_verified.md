# Naver Commerce API — Product Creation (상품 등록) VERIFIED

> Verified 2026-07-15 against live Naver SmartStore API
> Store: manuai | Test products: 13605038404, 13605041327

## Endpoint

```
POST https://api.commerce.naver.com/external/v2/products
Authorization: Bearer <token>
Content-Type: application/json
```

## Auth (see client.py)

```
ts = int((time.time() - 3) * 1000)
sign = base64(bcrypt.hashpw(f"{client_id}_{ts}", client_secret))
POST /external/v1/oauth2/token  (query params, NOT body)
  client_id, timestamp, client_secret_sign,
  grant_type=client_credentials, type=SELF
```

## Verified Working Payload Structure

```json
{
  "originProduct": {
    "statusType": "SALE",
    "saleType": "NEW",
    "leafCategoryId": "<from category API>",
    "name": "<max 100 chars>",
    "detailContent": "<HTML>",
    "images": {
      "representativeImage": {"url": "<Naver CDN URL>"},
      "optionalImages": [{"url": "<Naver CDN URL>"}]
    },
    "salePrice": 10000,
    "stockQuantity": 99,
    "deliveryInfo": {
      "deliveryType": "DELIVERY",
      "deliveryAttributeType": "NORMAL",
      "deliveryCompany": "CJGLS",
      "deliveryFee": {"deliveryFeeType": "FREE"},
      "claimDeliveryInfo": {
        "returnDeliveryFee": 2500,
        "exchangeDeliveryFee": 5000
      }
    },
    "detailAttribute": {
      "minorPurchasable": true,
      "originAreaInfo": {"originAreaCode": "00", "content": "일본", "plural": false},
      "afterServiceInfo": {
        "afterServiceTelephoneNumber": "02-0000-0000",
        "afterServiceGuideContent": "판매자에게 문의 바랍니다."
      },
      "optionInfo": {
        "simpleOptionSortType": "CREATE",
        "optionSimple": [],
        "optionCustom": [],
        "optionCombinationSortType": "CREATE",
        "standardOptionGroups": [],
        "optionStandards": [],
        "useStockManagement": true,
        "optionDeliveryAttributes": []
      },
      "purchaseReviewInfo": {"purchaseReviewExposure": true},
      "taxType": "TAX",
      "certificationTargetExcludeContent": {"greenCertifiedProductExclusionYn": true},
      "sellerCommentUsable": false,
      "productInfoProvidedNotice": {
        "productInfoProvidedNoticeType": "ETC",
        "etc": {
          "returnCostReason": "...",
          "noRefundReason": "...",
          "qualityAssuranceStandard": "관련 법 및 소비자 분쟁해결 기준에 따름",
          "compensationProcedure": "관련 법 및 소비자 분쟁해결 기준에 따름",
          "troubleShootingContents": "...",
          "itemName": "...",
          "modelName": "...",
          "manufacturer": "...",
          "customerServicePhoneNumber": "02-0000-0000"
        }
      },
      "itselfProductionProductYn": false,
      "unitCapacity": {"unitPriceYn": false}
    }
  },
  "smartstoreChannelProduct": {
    "naverShoppingRegistration": true,
    "channelProductDisplayStatusType": "ON"
  }
}
```

## Key Findings

### ❌ Things That Do NOT Work

| Field | Wrong Value | Why |
|---|---|---|
| `deliveryFeeType` | `"CHARGE"`, `"CHARGED"` | NotValidEnum — only `"FREE"` confirmed working |
| `deliveryCompany` | `"04"`, `"05"` (numeric) | Naver uses STRING codes: `CJGLS`, `HANJIN`, etc. |
| `originAreaCode` | `"02"` (overseas code) | Causes `ImportTypeNotFullySelected` error — use `"00"` |
| `productLogistics` | any `logisticsCompanyId` | Store-specific; public API has no lookup endpoint |

### ✅ Things That DO Work

| Field | Verified Value |
|---|---|
| `deliveryFeeType` | `"FREE"` (and `"FREE"` with `baseFee` > 0 works for 유료배송) |
| `deliveryCompany` | `"CJGLS"` (CJ대한통운) |
| `originAreaCode` | `"00"` — API resets to 국산 regardless |
| `leafCategoryId` | `"50002160"` — must be a valid leaf from `/external/v1/categories` |

### Required Fields That Are Easy to Miss

1. **`productInfoProvidedNotice`** — required. Use type `"ETC"` with all 10 sub-fields.
2. **`certificationTargetExcludeContent.greenCertificationExclusionYn`** — set `true` to skip.
3. **`unitCapacity.unitPriceYn`** — boolean `false` (required for some categories).
4. **`optionInfo`** — empty arrays are fine, but the object must exist.

### Category API

```
GET /external/v1/categories  → full category tree (5815 categories, 4999 leaf)
Filter by `last: true` to get leaf categories.
```

### Image Upload (must be done BEFORE product creation)

```
POST /external/v1/product-images/upload
  field name: "imageFiles"
  NO Content-Type header (httpx auto-sets with boundary)
  max 10 images
  → returns Naver CDN URLs (shop-phinf.pstatic.net)
```

### Overseas Purchase (해외구매대행) — KNOWN GAP

To create a product flagged as overseas purchase, you need `productLogistics[]`
with `overseasPurchaseType` and a store-specific `logisticsCompanyId`. The public
API does NOT expose a logistics company lookup endpoint. Without it, products are
created as domestic (국산). Next step: ask Naver Seller Support for the 물류사
연동 API, or use the product UPDATE endpoint after creation.

## Carrier Code Reference (Naver Commerce API specific)

| Code | Carrier |
|---|---|
| `CJGLS` | CJ대한통운 |
| `HANJIN` | 한진택배 |
| `LOGEN` | 로젠택배 |
| `POST` | 우체국택배 |
| `LOTTE` | 롯데택배 |

Source: https://apideveloper.naver.com/service_api_new/easy_tracking.html
