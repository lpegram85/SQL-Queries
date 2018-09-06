with Rev as(

		select 
		lar.LoanAcqQtrDt ,
	lar.SellerPartyId
	,lar.SellerLegalName
	,lar.SellerNum
	,lar.SalesCoordinator
	,LockMargin = 100*sum(case when LockBit = 1  then (coalesce(be.UpdatedMarginPrice,mr.MarginPrice)+ FeePrice) * ModAmt end)/nullif(sum(mr.LoanAmt*LockBit),0)
	,TotalRevenue = cast(((sum((coalesce(be.UpdatedMarginPrice,mr.MarginPrice) + mr.FeePrice)*mr.ModAmt)) * .01) as money)
	,TotalRevenueWithoutFee = cast(((sum(coalesce(be.UpdatedMarginPrice,mr.MarginPrice)*mr.ModAmt)) * .01) as money)
	from Magnus.dbo.loanacqrpt lar
	left join FinWrk.dbo.BeMarginAdj_Combined be
	on lar.loanid = be.loanid
	join FinWrk.dbo.MarginRpt_Combined mr
	on lar.SellerLoanNumTxt = mr.SellerLoanNumTxt 
	where  LoanAcqQtrDt >='2017-01-01'
	and LoanAcqQtrDt <=getdate()
	group by lar.SellerLegalName
	,lar.SellerPartyId
	,lar.SellerNum
	,lar.SalesCoordinator
	,lar.LoanAcqQtrDt
	
)



select 
[Quarter Year]
 ,sellerpartyid,
-- Activebit,
[Seller Legal Name],
[Sales Exec],
Client_Tier
,GovtPercent
,ConvPercent
,[Client Vintage]
,[Wind-down Complete & Termination Date]
,[Active in This Time Frame]
,[Total Revenue]
,ConvTurnTimes
,GovtTurnTimes
,ConvRatio
,DelinquencyRatio
,[Sum of NetMsrYld Conv]
,[Sum of NetMsrYld Govt]

,case when([Total Revenue]>=percentile_cont (0.75) WITHIN GROUP
		(ORDER BY [Total Revenue] ASC) OVER(PARTITION BY [Quarter Year])) then 1
	when([Total Revenue]<( percentile_cont (0.75)  WITHIN GROUP
		(ORDER BY [Total Revenue] ASC) OVER(PARTITION BY [Quarter Year])) and [Total Revenue]>=(percentile_cont (0.50)  WITHIN GROUP
		(ORDER BY [Total Revenue] ASC) OVER(PARTITION BY [Quarter Year]))) then 2
	when([Total Revenue]<( percentile_cont (0.50)  WITHIN GROUP
		(ORDER BY [Total Revenue] ASC) OVER(PARTITION BY [Quarter Year])) and [Total Revenue]>=(percentile_cont (0.25)  WITHIN GROUP
		(ORDER BY [Total Revenue] ASC) OVER(PARTITION BY [Quarter Year]))) then 3
		else 4 end as Quart_Revenue
,case when(ConvTurnTimes>=percentile_cont (0.75) WITHIN GROUP
		(ORDER BY ConvTurnTimes ASC) OVER(PARTITION BY [Quarter Year])) then 1
	when(ConvTurnTimes<( percentile_cont (0.75)  WITHIN GROUP
		(ORDER BY ConvTurnTimes ASC) OVER(PARTITION BY [Quarter Year])) and ConvTurnTimes>=(percentile_cont (0.50)  WITHIN GROUP
		(ORDER BY ConvTurnTimes ASC) OVER(PARTITION BY [Quarter Year]))) then 2
	when(ConvTurnTimes<( percentile_cont (0.50)  WITHIN GROUP
		(ORDER BY ConvTurnTimes ASC) OVER(PARTITION BY [Quarter Year])) and ConvTurnTimes>=(percentile_cont (0.25)  WITHIN GROUP
		(ORDER BY ConvTurnTimes ASC) OVER(PARTITION BY [Quarter Year]))) then 3
		when ConvTurnTimes is null then null
		else 4 end as Conv_TurnTimeQuart

,case when(GovtTurnTimes>=percentile_cont (0.75) WITHIN GROUP
		(ORDER BY GovtTurnTimes ASC) OVER(PARTITION BY [Quarter Year])) then 1
	when(GovtTurnTimes<( percentile_cont (0.75)  WITHIN GROUP
		(ORDER BY GovtTurnTimes ASC) OVER(PARTITION BY [Quarter Year])) and GovtTurnTimes>=(percentile_cont (0.50)  WITHIN GROUP
		(ORDER BY GovtTurnTimes ASC) OVER(PARTITION BY [Quarter Year]))) then 2
	when(GovtTurnTimes<( percentile_cont (0.50)  WITHIN GROUP
		(ORDER BY GovtTurnTimes ASC) OVER(PARTITION BY [Quarter Year])) and GovtTurnTimes>=(percentile_cont (0.25)  WITHIN GROUP
		(ORDER BY GovtTurnTimes ASC) OVER(PARTITION BY [Quarter Year]))) then 3
		when GovtTurnTimes is null then null
		else 4 end as Govt_TurnTimeQuart

,case when([Sum of NetMsrYld Govt]>=percentile_cont (0.75) WITHIN GROUP
		(ORDER BY [Sum of NetMsrYld Govt] ASC) OVER(PARTITION BY [Quarter Year])) then 1
	when([Sum of NetMsrYld Govt]<( percentile_cont (0.75)  WITHIN GROUP
		(ORDER BY [Sum of NetMsrYld Govt] ASC) OVER(PARTITION BY [Quarter Year])) and [Sum of NetMsrYld Govt]>=(percentile_cont (0.50)  WITHIN GROUP
		(ORDER BY [Sum of NetMsrYld Govt] ASC) OVER(PARTITION BY [Quarter Year]))) then 2
	when([Sum of NetMsrYld Govt]<( percentile_cont (0.50)  WITHIN GROUP
		(ORDER BY [Sum of NetMsrYld Govt] ASC) OVER(PARTITION BY [Quarter Year])) and [Sum of NetMsrYld Govt]>=(percentile_cont (0.25)  WITHIN GROUP
		(ORDER BY [Sum of NetMsrYld Govt] ASC) OVER(PARTITION BY [Quarter Year]))) then 3
		when [Sum of NetMsrYld Govt] is null then null
		else 4 end as Govt_MSRYieldQuart
,case when([Sum of NetMsrYld Conv]>=percentile_cont (0.75) WITHIN GROUP
		(ORDER BY [Sum of NetMsrYld Conv] ASC) OVER(PARTITION BY [Quarter Year])) then 1
	when([Sum of NetMsrYld Conv]<( percentile_cont (0.75)  WITHIN GROUP
		(ORDER BY [Sum of NetMsrYld Conv] ASC) OVER(PARTITION BY [Quarter Year])) and [Sum of NetMsrYld Conv]>=(percentile_cont (0.50)  WITHIN GROUP
		(ORDER BY [Sum of NetMsrYld Conv] ASC) OVER(PARTITION BY [Quarter Year]))) then 2
	when([Sum of NetMsrYld Conv]<( percentile_cont (0.50)  WITHIN GROUP
		(ORDER BY [Sum of NetMsrYld Conv] ASC) OVER(PARTITION BY [Quarter Year])) and [Sum of NetMsrYld Conv]>=(percentile_cont (0.25)  WITHIN GROUP
		(ORDER BY [Sum of NetMsrYld Conv] ASC) OVER(PARTITION BY [Quarter Year]))) then 3
		when [Sum of NetMsrYld Conv] is null then null
		else 4 end as Net_MSR_Conv_Quart
,case when(ConvRatio>=0 and ConvRatio<1) then 1
	when(ConvRatio>=1 and ConvRatio<2) then 2
	when(ConvRatio>=2) then 3
		else null end as Conv_DelinquencyTT
		
,case when
(DelinquencyRatio>=0 and DelinquencyRatio<1) then 1
	when(DelinquencyRatio>=1 and DelinquencyRatio<2) then 2
	when(DelinquencyRatio>=2) then 3
		else null end as Govt_RatioQuart
from(

select 
--distinct dt.pivotdate
--[Quarter Year]=case when datepart(month, dt.pivotdate) >=1 and datepart(month, dt.pivotdate)<=3 then concat(datepart(year,pivotdate),' Q1')
--when datepart(month,dt.pivotdate) >=4 and datepart(month,dt.pivotdate)<=6 then concat(datepart(year,pivotdate),' Q2')
--when datepart(month,dt.pivotdate) >=7 and datepart(month,dt.pivotdate)<=9 then concat(datepart(year,pivotdate),' Q3')
--when datepart(month,dt.pivotdate) >=10 and datepart(month,dt.pivotdate)<=12 then concat(datepart(year,pivotdate),' Q4') end
[Quarter Year]=pivotdate
 --,Activebit=coalesce(lar.numberofloans,0)
 ,sales.sellerpartyid,
[Seller Legal Name],
[Sales Exec],
Client_Tier
,GovtPercent=coalesce(cg.GovtPercent,0.0)--if conv percent =0 then 0 
,ConvPercent=coalesce(cg.ConvPercent,0.0) --if govt percent =0 then 0 
,[Activation Month]=case when [Activation Date] is not null then convert(date,concat(substring(convert(varchar,[Activation Date],120),1,7),'-01'),120) else null end 
,[Client Vintage]=datediff(MONTH,[Activation Date],DATEADD(month, 3, dt.pivotdate))
,[Wind-down Complete & Termination Date]
,[Active in This Time Frame]=case when [Wind-down Complete & Termination Date] is null and convert(date,concat(substring(convert(varchar,[Activation Date],120),1,7),'-01'),120) is not null then 1
when (convert(date,concat(substring(convert(varchar,[Activation Date],120),1,7),'-01'),120) is not null) and (convert(date,concat(substring(convert(varchar,[Activation Date],120),1,7),'-01'),120) <lar.LoanAcqQtrDt) then 1 else 0 end
,[Principal Balance]=coalesce(lar.prinbal,0)
,[Total Revenue]=coalesce(rev.TotalRevenue,0)
--,ConvTurnTimes=coalesce(ConvTurnTimes,20)
,ConvTurnTimes=ConvTurnTimes
--,GovtTurnTimes=coalesce(GovtTurnTimes,20)
,GovtTurnTimes=GovtTurnTimes
--,ConvRatio= coalesce(Mba60PlusDaysDlqActualvsExpectedRatio_Agency_ExcludingDisasterImpactedCounties,10)--should averagethis
--,DelinquencyRatio=coalesce(Mba60PlusDaysDlqActualvsExpectedRatio_Govt_ExcludingDisasterImpactedCounties,10)
,ConvRatio= Mba60PlusDaysDlqActualvsExpectedRatio_Agency_ExcludingDisasterImpactedCounties--should averagethis
,DelinquencyRatio=Mba60PlusDaysDlqActualvsExpectedRatio_Govt_ExcludingDisasterImpactedCounties
--,[Sum of NetMsrYld Conv]=coalesce(cast(msr.[Sum of NetMsrYld]as float),0 )
,[Sum of NetMsrYld Conv]=cast(msr.[Sum of NetMsrYld]as float)
--,[Sum of NetMsrYld Govt]=coalesce(cast(msr2.[Sum of NetMsrYld] as float),0)
,[Sum of NetMsrYld Govt]=cast(msr2.[Sum of NetMsrYld] as float)
from (select QtrDt as pivotdate from dt) dt
left outer join (select LoanAcqQtrDt
, sellerpartyid
, sum(LoanAcqPrinBalAmt) as PrinBal
,count(loanid) as numberofloans
from Magnus.dbo.loanacqrpt 
group by LoanAcqQtrDt, sellerpartyid
)lar
on dt.pivotdate=lar.LoanAcqQtrDt
left join Rev
on dt.pivotdate=rev.LoanAcqQtrDt
and lar.sellerpartyid=rev.sellerpartyid
join 
Salesforce.dbo.CounterpartyRisk sales
on sales.SellerPartyID=lar.SellerPartyId
left join (	select 
	lar.sellerpartyid,
	LoanAcqQtrDt,
	ConvTurnTimes = avg(case when lar.underwritingprogrollup1grp='Conv' then coalesce(e.FileReceivedtoreadytopurchasedays,0) end),
	GovtTurnTimes=avg(case when lar.underwritingprogrollup1grp='Govt' then coalesce(e.FileReceivedtoreadytopurchasedays,0) end)
From Magnus.dbo.EzCorrespondentMilestoneLoan e --gotta stack the loans 
join Magnus.dbo.loanAcqRpt lar
on lar.LoanId = e.LoanId
where lar.LoanAcqQtrDt > '2017-01-01'
group by sellerpartyid,
		LoanAcqQtrDt) tt
on dt.pivotdate=tt.LoanAcqQtrDt
and lar.sellerpartyid=tt.sellerpartyid

left join (
select 
d.QtrDt
,r.* 
from [Scratch].[dbo].[ClientRiskScoreCardKeyData] r
join [Magnus].[dbo].dt d 
on r.asofdate=d.dt 
)dlq--repeats for now
on lar.sellerpartyid=dlq.sellerpartyid
--and dt.pivotdate=dlq.QtrDt

left join [Scratch].[dbo].[MSR_Q2_Conv2018] msr-------------------------------upload newest version
on lar.sellerpartyid=msr.sellerpartyid
and lar.LoanAcqQtrDt=cast(msr.qtrdt as date) 
left join [Scratch].[dbo].[MSR_Q2_Govt2018] msr2------------------------------- upload newest version
on lar.sellerpartyid=msr2.sellerpartyid
and lar.LoanAcqQtrDt=cast(msr2.qtrdt as date) 
Left join (
					select 
				l.LoanAcqQtrDt,
				l.sellerpartyid
				,coalesce(govtloans,0)govtloans
				,coalesce(convloans,0) convloans
				,sum(l.LoanAcqPrinBalAmt) Totalacq
				,case when sum(l.LoanAcqPrinBalAmt) >150000000 then 'Extra_Large'
				when sum(l.LoanAcqPrinBalAmt)>75000000 and sum(l.LoanAcqPrinBalAmt)<=150000000 then 'Large'
				when sum(l.LoanAcqPrinBalAmt)>=25000000 and sum(l.LoanAcqPrinBalAmt)<=750000000 then 'Medium'
				when sum(l.LoanAcqPrinBalAmt)<25000000 then 'Small'
				else null end as Client_Tier
				,coalesce(govtloans/sum(l.LoanAcqPrinBalAmt),0) GovtPercent
				,coalesce(convloans/sum(l.LoanAcqPrinBalAmt),0) ConvPercent
				from Magnus.dbo.loanacqrpt l
				left join(

				select sellerpartyid
				,LoanAcqQtrDt
				,sum(LoanAcqPrinBalAmt) govtloans
				from Magnus.dbo.loanacqrpt
				where UnderwritingProgRollup1Grp='Govt'
				and LoanAcqQtrDt >='2017-01-01'
				and LoanAcqQtrDt <=getdate()
				group by sellerpartyid,LoanAcqQtrDt
				)g
				on l.SellerPartyId=g.SellerPartyId
				and l.LoanAcqQtrDt=g.LoanAcqQtrDt
				left join(

				select sellerpartyid
				,LoanAcqQtrDt
				,sum(LoanAcqPrinBalAmt) convloans
				from  Magnus.dbo.loanacqrpt
				where UnderwritingProgRollup1Grp='Conv'
		and LoanAcqQtrDt >='2017-01-01'
				and LoanAcqQtrDt <=getdate()
				group by sellerpartyid,LoanAcqQtrDt
				
				)c
				on l.SellerPartyId=c.SellerPartyId
				and l.LoanAcqQtrDt=c.LoanAcqQtrDt
				where  l.LoanAcqQtrDt >='2017-01-01'
				and l.LoanAcqMonthDt <=getdate()
				group by 
				l.sellerpartyid,
				l.LoanAcqQtrDt		
				,govtloans		
				,convloans
			) cg
on dt.pivotdate=cg.LoanAcqQtrDt
and lar.sellerpartyid=cg.sellerpartyid
where [Activation Date] is not null
and dt.pivotdate<= getdate()
and dt.pivotdate>='2017-01-01'
group by dt.pivotdate
--case when datepart(month, dt.pivotdate) >=1 and datepart(month, dt.pivotdate)<=3 then concat(datepart(year,pivotdate),' Q1')
--when datepart(month,dt.pivotdate) >=4 and datepart(month,dt.pivotdate)<=6 then concat(datepart(year,pivotdate),' Q2')
--when datepart(month,dt.pivotdate) >=7 and datepart(month,dt.pivotdate)<=9 then concat(datepart(year,pivotdate),' Q3')
--when datepart(month,dt.pivotdate) >=10 and datepart(month,dt.pivotdate)<=12 then concat(datepart(year,pivotdate),' Q4') end
,lar.numberofloans
,sales.sellerpartyid,
[Seller Legal Name],
[Sales Exec],
Client_Tier
,cg.GovtPercent
,cg.ConvPercent
,case when [Activation Date] is not null then convert(date,concat(substring(convert(varchar,[Activation Date],120),1,7),'-01'),120) else null end 
,datediff(MONTH,[Activation Date],DATEADD(month, 3, dt.pivotdate))
,[Wind-down Complete & Termination Date]
,case when [Wind-down Complete & Termination Date] is null and convert(date,concat(substring(convert(varchar,[Activation Date],120),1,7),'-01'),120) is not null then 1
when (convert(date,concat(substring(convert(varchar,[Activation Date],120),1,7),'-01'),120) is not null) and (convert(date,concat(substring(convert(varchar,[Activation Date],120),1,7),'-01'),120) <lar.LoanAcqQtrDt) then 1 else 0 end
,lar.prinbal
,rev.TotalRevenue
,ConvTurnTimes
,GovtTurnTimes
,Mba60PlusDaysDlqActualvsExpectedRatio_Agency_ExcludingDisasterImpactedCounties--should averagethis
,Mba60PlusDaysDlqActualvsExpectedRatio_Govt_ExcludingDisasterImpactedCounties
,msr.[Sum of NetMsrYld]
,msr2.[Sum of NetMsrYld])a
group by [Quarter Year] 
,sellerpartyid,
[Seller Legal Name],
[Sales Exec],
Client_Tier
,GovtPercent
,ConvPercent
,[Client Vintage]
,[Wind-down Complete & Termination Date]
,[Active in This Time Frame]
,[Total Revenue]
,ConvTurnTimes
,GovtTurnTimes
,ConvRatio
,DelinquencyRatio
,[Sum of NetMsrYld Conv]
,[Sum of NetMsrYld Govt]
order by 2 desc


--select * from [ClientRiskScoreCardKeyData]