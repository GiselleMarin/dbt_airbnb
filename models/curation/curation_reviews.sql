WITH curation_raw AS (
    SELECT
        listing_id,
        date AS review_date
    FROM {{ source("raw_airbnb_data","reviews")}}
    WHERE listing_id in (select listing_id from {{ ref("curation_listings")}})
)
SELECT
    listing_id,
    review_date,
    COUNT(*) AS number_reviews
FROM curation_raw
GROUP BY 1, 2