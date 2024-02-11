-- Исходные датасет
with cte as (select distinct on (visit_date, utm_source, utm_medium, utm_campaign)
	to_char(s.visit_date, 'YYYY-MM-DD') as visit_date,
	to_char(s.visit_date, 'Day') as weekday,
	extract(isodow from s.visit_date) - 1 as daycode,
	s.source as utm_source,
	s.medium as utm_medium,
	s.campaign as utm_campaign,
	count(s.visitor_id) as visitors_count,
	count(l.lead_id) as leads_count,
	count(case when l.closing_reason = 'Успешно реализовано' or l.status_id = 142 then 1 end) as purchases_count,
	sum(l.amount) as revenue
from sessions s
left join leads l on s.visitor_id = l.visitor_id and s.visit_date <= l.created_at
where s.medium in ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
group by to_char(s.visit_date, 'YYYY-MM-DD'), to_char(s.visit_date, 'Day'), extract(isodow from s.visit_date) - 1, s.source, s.medium, s.campaign
order by 
	visit_date desc
), ads as (
	select to_char(campaign_date, 'YYYY-MM-DD') as campaign_date, utm_source, utm_medium, utm_campaign, sum(daily_spent) as total_cost
	from vk_ads
	group by to_char(campaign_date, 'YYYY-MM-DD'), utm_source, utm_medium, utm_campaign
	union
	select to_char(campaign_date, 'YYYY-MM-DD') as campaign_date, utm_source, utm_medium, utm_campaign, sum(daily_spent) as total_cost
	from ya_ads
	group by to_char(campaign_date, 'YYYY-MM-DD'), utm_source, utm_medium, utm_campaign
)
select 
	cte.visit_date,
	cte.utm_source,
	cte.utm_medium,
	cte.utm_campaign,
	cte.visitors_count,
	ads.total_cost,
	cte.leads_count,
	cte.purchases_count,
	cte.revenue,
	cte.weekday,
	cte.daycode
from cte left join ads on lower(cte.utm_source) = lower(ads.utm_source) and
	lower(cte.utm_medium) = lower(ads.utm_medium) and lower(cte.utm_campaign) = lower(ads.utm_campaign) and cte.visit_date = ads.campaign_date
order by revenue desc nulls last, visit_date, visitors_count desc, utm_source, utm_medium, utm_campaign
;

-- CUSTOM SQL для поля "cpu" в таблице "Метрики". Аналогичные расчеты для других полей таблицы из исходного датасета
case when sum(visitors_count) = 0 then 0 else round(sum(total_cost) / sum(visitors_count), 2) end

-- CUSTOM SQL для поля "выручка" в таблице "Окупаемость рекламы"
SUM(case when revenue is not null then revenue else 0 end)

-- CUSTOM SQL для поля "прибыль" в таблице "Окупаемость рекламы"
SUM(case when revenue is not null then revenue else 0 end) - sum(total_cost)
