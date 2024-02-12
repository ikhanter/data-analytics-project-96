-- Исходный датасет
with cte0 as (
  select 
    distinct on (s.visitor_id) s.visitor_id as visitor_id, 
    s.visit_date as visit_date, 
    s.source as utm_source, 
    s.medium as utm_medium, 
    s.campaign as utm_campaign, 
    l.lead_id as lead_id, 
    l.created_at as created_at, 
    l.amount as amount, 
    l.closing_reason as closing_reason, 
    l.status_id as status_id 
  from 
    sessions as s 
    left join leads as l on s.visitor_id = l.visitor_id 
    and s.visit_date <= l.created_at 
  where 
    s.medium in (
      'cpc', 'cpm', 'cpa', 'youtube', 'cpp', 
      'tg', 'social'
    ) 
  order by 
    s.visitor_id asc, 
    s.visit_date desc
), 
cte1 as (
  select 
    distinct on (
      visit_date, utm_source, utm_medium, 
      utm_campaign
    ) to_char(visit_date, 'YYYY-MM-DD') as visit_date, 
    utm_source, 
    utm_medium, 
    utm_campaign, 
    count(visitor_id) as visitors_count, 
    count(lead_id) as leads_count, 
    count(
      case when closing_reason = 'Успешно реализовано' 
      or status_id = 142 then 1 end
    ) as purchases_count, 
    sum(amount) as revenue 
  from 
    cte0 
  group by 
    to_char(visit_date, 'YYYY-MM-DD'), 
    utm_source, 
    utm_medium, 
    utm_campaign 
  order by 
    visit_date desc
), 
ads as (
  select 
    to_char(campaign_date, 'YYYY-MM-DD') as campaign_date, 
    utm_source, 
    utm_medium, 
    utm_campaign, 
    sum(daily_spent) as total_cost 
  from 
    vk_ads 
  group by 
    to_char(campaign_date, 'YYYY-MM-DD'), 
    utm_source, 
    utm_medium, 
    utm_campaign 
  union 
  select 
    to_char(campaign_date, 'YYYY-MM-DD') as campaign_date, 
    utm_source, 
    utm_medium, 
    utm_campaign, 
    sum(daily_spent) as total_cost 
  from 
    ya_ads 
  group by 
    to_char(campaign_date, 'YYYY-MM-DD'), 
    utm_source, 
    utm_medium, 
    utm_campaign
) 
select 
  cte1.visit_date, 
  cte1.visitors_count, 
  cte1.utm_source, 
  cte1.utm_medium, 
  cte1.utm_campaign, 
  ads.total_cost, 
  cte1.leads_count, 
  cte1.purchases_count, 
  cte1.revenue 
from 
  cte1 
  left join ads on lower(cte1.utm_source) = lower(ads.utm_source) 
  and lower(cte1.utm_medium) = lower(ads.utm_medium) 
  and lower(cte1.utm_campaign) = lower(ads.utm_campaign) 
  and cte1.visit_date = ads.campaign_date 
order by 
  revenue desc nulls last, 
  visit_date, 
  visitors_count desc, 
  utm_source, 
  utm_medium, 
  utm_campaign;
-- CUSTOM SQL для поля "cpu" в таблице "Метрики". Аналогичные расчеты для других полей таблицы из исходного датасета
case when sum(visitors_count) = 0 then 0 else round(
  sum(total_cost) / sum(visitors_count), 
  2
) end -- CUSTOM SQL для поля "выручка" в таблице "Окупаемость рекламы"
SUM(
  case when revenue is not null then revenue else 0 end
) -- CUSTOM SQL для поля "прибыль" в таблице "Окупаемость рекламы"
SUM(
  case when revenue is not null then revenue else 0 end
) - sum(total_cost)
