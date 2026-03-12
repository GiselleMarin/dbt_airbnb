-- Analyse 4 : Touristes Airbnb vs Hôtels à Amsterdam
-- Hypothèses :
--   H1 : 1 séjour Airbnb = 1 review (taux = 1.0)
--   H2 : 1 séjour Airbnb = 0.8 review (taux = 0.8)


with reviews_par_annee as (
    select
    year(review_date) as annee,
    count(*) as nb_reviews
    from {{ ref('curation_reviews') }}
    group by 1
),

touristes as (
    select
    year(year) as annee,
    tourists as nb_touristes
    from {{ ref('curation_tourists_per_year') }}
),


calcul as (
    select
        t.annee,
        t.nb_touristes,
        coalesce(r.nb_reviews, 0) as nb_reviews,

        -- Scénario H1 : taux = 1.0
        {{ sejours_estimes('coalesce(r.nb_reviews, 0)', 1.0) }} as sejours_airbnb_h1,
        {{ pct_airbnb(sejours_estimes('coalesce(r.nb_reviews, 0)', 1.0), 't.nb_touristes') }} as pct_airbnb_h1,

        -- Scénario H2 : taux = 0.8
        {{ sejours_estimes('coalesce(r.nb_reviews, 0)', 0.8) }} as sejours_airbnb_h2,
        {{ pct_airbnb(sejours_estimes('coalesce(r.nb_reviews, 0)', 0.8), 't.nb_touristes') }} as pct_airbnb_h2

    from touristes t
    left join reviews_par_annee r on t.annee = r.annee
),

evolution as (
    select
        *,
        lag(pct_airbnb_h1) over (order by annee) as pct_h1_annee_prec,
        lag(pct_airbnb_h2) over (order by annee) as pct_h2_annee_prec,
        pct_airbnb_h1 - lag(pct_airbnb_h1) over (order by annee)  as delta_h1,
        pct_airbnb_h2 - lag(pct_airbnb_h2) over (order by annee)  as delta_h2

    from calcul
)

select
    annee,
    nb_touristes,
    nb_reviews,
    sejours_airbnb_h1,
    sejours_airbnb_h2,
    pct_airbnb_h1,
    pct_airbnb_h2,
    round(delta_h1, 2) as evolution_pct_h1,
    round(delta_h2, 2) as evolution_pct_h2


from evolution
order by annee
