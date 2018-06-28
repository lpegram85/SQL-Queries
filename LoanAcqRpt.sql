  
  
  
------------------------------------------------------------------------------------------------------------------------------------------  
CREATE view dbo.LoanAcqRpt  
as  
  with LoanConditionCnt as (  
   select lcd.LoanId ,  
    ClientMetricsTotalEverStipCnt = count(*)  
   from LoanCondition lcd  
   join LoanSys lsys  
    on lcd.LoanId = lsys.LoanId  
   where lcd.InternalBit = 0   
    and lcd.ConditionId <> 126  
    and lsys.EncompassBit = 1  
    and lcd.ConditionStatusTypeId <> 9  
   group by lcd.LoanId),  
  LoanStipDtl_All as (  
   select lst.LoanId, ClientMetricsTotalEverStipCnt  
   from LoanStipDtl lst  
   join LoanSys lsys  
    on lst.LoanId = lsys.LoanId  
   where lsys.EncompassBit = 0  
  
   union   
   select lcdcnt.LoanId , lcdcnt.ClientMetricsTotalEverStipCnt  
   from LoanConditionCnt lcdcnt  
   ),  
   LoanMileStoneDtl_All as (  
    select lmd.LoanId,  
       elmd.FirstPurchaseReviewStartedDtTm,  
       lmd.BaseFileReceivedDtTm,  
       PendingConditionsReachedBit = case when (lsys.EncompassBit = 1 and lc.ChannelTypeId = 1) then elmd.ConditionsReviewReachedBit else lmd.PendingConditionsReachedBit end,  
       SubmittedForFundingReviewReachedBit = case when (lsys.EncompassBit = 1 and lc.ChannelTypeId = 1) then elmd.PurchaseReviewReachedBit else lmd.SubmittedForFundingReviewReachedBit end,  
       PendingConditionsDtTm = case when (lsys.EncompassBit = 1 and lc.ChannelTypeId = 1) then elmd.LastConditionsReviewStartedDtTm else lmd.PendingConditionsDtTm end,  
       SubmittedForFundingReviewDtTm = case when (lsys.EncompassBit = 1 and lc.ChannelTypeId = 1) then elmd.FirstPurchaseReviewStartedDtTm else lmd.SubmittedForFundingReviewDtTm end,  
       ApprovedForPurchaseDtTm = case when (lsys.EncompassBit = 1 and lc.ChannelTypeId = 1) then elmd.FirstReadyForPurchaseStartedDtTm else lmd.ApprovedForPurchaseDtTm end,  
       BaseFileReceivedToSubmittedForAuditCalDays = case when (lsys.EncompassBit = 1 and lc.ChannelTypeId = 1) then enc_base_file_recieved_to_submitAudit_cald.Days else lmd.BaseFileReceivedToSubmittedForAuditCalDays end,  
       BaseFileReceivedToSubmittedForAuditBusDays = case when (lsys.EncompassBit = 1 and lc.ChannelTypeId = 1) then enc_base_file_recieved_to_submitAudit_bd.Days else lmd.BaseFileReceivedToSubmittedForAuditBusDays end,  
       BaseFileReceivedToPendingConditionsCalDays = case when (lsys.EncompassBit = 1 and lc.ChannelTypeId = 1) then enc_base_file_recieved_to_pendingcond_cald.Days else lmd.BaseFileReceivedToPendingConditionsCalDays end,  
       BaseFileReceivedToPendingConditionsBusDays = case when (lsys.EncompassBit = 1 and lc.ChannelTypeId = 1) then enc_base_file_recieved_to_pendingcond_bd.Days else lmd.BaseFileReceivedToPendingConditionsBusDays end,  
       PendingConditionsToSubmittedForFundingReviewCalDays = case when (lsys.EncompassBit = 1 and lc.ChannelTypeId = 1) then enc_pendcond_to_fundingreview_cald.Days else lmd.BaseFileReceivedToPendingConditionsCalDays end,  
       PendingConditionsToSubmittedForFundingReviewBusDays = case when (lsys.EncompassBit = 1 and lc.ChannelTypeId = 1) then enc_pendcond_to_fundingreview_bd.Days else lmd.PendingConditionsToSubmittedForFundingReviewBusDays end,  
       SubmittedForFundingReviewToApprovedForPurchaseCalDays = case when (lsys.EncompassBit = 1 and lc.ChannelTypeId = 1) then enc_FundingReview_to_purchased_cald.Days else lmd.SubmittedForFundingReviewToApprovedForPurchaseCalDays end,  
       SubmittedForFundingReviewToApprovedForPurchaseBusDays = case when (lsys.EncompassBit = 1 and lc.ChannelTypeId = 1) then enc_FundingReview_to_purchased_bd.Days else lmd.SubmittedForFundingReviewToApprovedForPurchaseCalDays end,  
       BaseFileReceivedToApprovedForPurchaseCalDays = case when (lsys.EncompassBit = 1 and lc.ChannelTypeId = 1) then enc_base_file_recieved_to_purchased_cald.Days else lmd.BaseFileReceivedToApprovedForPurchaseCalDays end,   
       BaseFileReceivedToApprovedForPurchaseBusDays = case when(lsys.EncompassBit = 1 and lc.ChannelTypeId = 1) then enc_base_file_recieved_to_purchased_bd.Days else lmd.BaseFileReceivedToApprovedForPurchaseBusDays end  
    from LoanMilestoneDtl lmd  
    join LoanSys lsys  
     on lmd.LoanId = lsys.LoanId  
    join LoanChannel lc  
     on lmd.LoanId = lc.LoanId   
    left join CorrespondentMilestoneLoan elmd  
     on lmd.LoanId = elmd.LoanId  
    outer apply dbo.DecimalDays(lmd.BaseFileReceivedDtTm, elmd.FirstReadyForPurchaseStartedDtTm, 'B') enc_base_file_recieved_to_purchased_bd  
    outer apply dbo.DecimalDays(elmd.FirstPurchaseReviewStartedDtTm, elmd.FirstReadyForPurchaseStartedDtTm, 'B') enc_FundingReview_to_purchased_bd  
    outer apply dbo.DecimalDays(elmd.LastConditionsReviewStartedDtTm, elmd.FirstPurchaseReviewStartedDtTm, 'B') enc_pendcond_to_fundingreview_bd  
    outer apply dbo.DecimalDays(lmd.BaseFileReceivedDtTm, elmd.FirstAuditReviewStartedDtTm, 'B') enc_base_file_recieved_to_submitAudit_bd  
    outer apply dbo.DecimalDays(lmd.BaseFileReceivedDtTm, elmd.LastConditionsReviewStartedDtTm, 'B') enc_base_file_recieved_to_pendingcond_bd  
  
    outer apply dbo.DecimalCalDays(lmd.BaseFileReceivedDtTm, elmd.FirstReadyForPurchaseStartedDtTm) enc_base_file_recieved_to_purchased_cald  
    outer apply dbo.DecimalCalDays(elmd.FirstPurchaseReviewStartedDtTm, elmd.FirstReadyForPurchaseStartedDtTm) enc_FundingReview_to_purchased_cald  
    outer apply dbo.DecimalCalDays(elmd.LastConditionsReviewStartedDtTm, elmd.FirstPurchaseReviewStartedDtTm) enc_pendcond_to_fundingreview_cald  
    outer apply dbo.DecimalCalDays(lmd.BaseFileReceivedDtTm, elmd.FirstAuditReviewStartedDtTm) enc_base_file_recieved_to_submitAudit_cald  
    outer apply dbo.DecimalCalDays(lmd.BaseFileReceivedDtTm, elmd.LastConditionsReviewStartedDtTm) enc_base_file_recieved_to_pendingcond_cald  
   ),  
        a as (  
            select  
                l.LoanId,  
                l.LoanNum,  
    plb.LastName,  
                FundedTodayBit = convert(bit, case  
                    when la.LoanAcqDt = convert(date, getdate()) then 1  
                    else 0  
                    end),  
                lc.ChannelTypeId,  
                lc.ChannelTypeName,  
                ll.CommitmentTypeId,  
                ll.CommitmentTypeName,  
                lc.SellerPartyId,  
    lc.SellerNum,  
                lc.SellerLoanNumTxt,  
                lc.SellerLegalName,  
    SalesCoordinator = coalesce(lr.FullName,ezp.FullName),  
                ll.InitialLockDtTm,  
                ll.InitialLockDt,  
                ll.InitialLockDays,  
                ll.LockDays,  
                LockedToFundedCalDays = datediff(day, ll.InitialLockDt, la.LoanAcqDt),  
                LockedToFundedBusDays = LockedToFundBusDays.Days,  
                LockedToBaseFileReceivedCalDays = datediff(day, ll.InitialLockDt, lmd.BaseFileReceivedDtTm),  
                LockedToBaseFileReceivedBusDays = LockedToDelivery.Days,  
                la.LoanAcqDtTm,  
                la.LoanAcqDt,  
                la.LoanAcqWeekDt,  
                la.LoanAcqMonthDt,  
                la.LoanAcqQtrDt,  
                la.LoanAcqYrDt,  
                la.LoanAcqPrinBalAmt,  
                la.LoanAcqFedWireAmt,  
                lu.UnderwritingProgRollup1Grp,  
                lu.UnderwritingProgRollup2Grp,  
                lu.UnderwritingProgRollup3Grp,  
               lu.UnderwritingProgName,  
                lu.ScratchAndDentBit,  
    lt.LoanPurposeTypeName,  
    lt.RefiTypeName,  
    at.AmortTypeName,  
                lpa.PropStateCd,  
    lp.PropUsageTypeName,  
    lp.PropTypeName,  
    lp.PropUnitsCnt,  
                lmd.PendingConditionsReachedBit,  
                lmd.SubmittedForFundingReviewReachedBit,  
                lmd.BaseFileReceivedDtTm,  
                lmd.PendingConditionsDtTm,  
                lmd.SubmittedForFundingReviewDtTm,  
                lmd.ApprovedForPurchaseDtTm,  
                BaseFileReceivedToSubmittedForAuditCalDays = isnull(lmd.BaseFileReceivedToSubmittedForAuditCalDays, 0),  
                BaseFileReceivedToSubmittedForAuditCalDaysGrp = case  
                    when isnull(lmd.BaseFileReceivedToSubmittedForAuditCalDays, 0) = 0 then '[0]'  
                    when lmd.BaseFileReceivedToSubmittedForAuditCalDays <= 1 then '(0 - 1]'  
                    when lmd.BaseFileReceivedToSubmittedForAuditCalDays <= 2 then '(1 - 2]'  
                    when lmd.BaseFileReceivedToSubmittedForAuditCalDays <= 3 then '(2 - 3]'  
                    when lmd.BaseFileReceivedToSubmittedForAuditCalDays <= 5 then '(3 - 5]'  
                    when lmd.BaseFileReceivedToSubmittedForAuditCalDays <= 10 then '(5 - 10]'  
                    when lmd.BaseFileReceivedToSubmittedForAuditCalDays <= 15 then '(10 - 15]'  
                    when lmd.BaseFileReceivedToSubmittedForAuditCalDays <= 20 then '(15 - 20]'  
                    when lmd.BaseFileReceivedToSubmittedForAuditCalDays <= 25 then '(20 - 25]'  
                    when lmd.BaseFileReceivedToSubmittedForAuditCalDays > 25 then '> 25'  
                    end,  
                BaseFileReceivedToSubmittedForAuditBusDays = isnull(lmd.BaseFileReceivedToSubmittedForAuditBusDays, 0),  
                BaseFileReceivedToSubmittedForAuditBusDaysGrp = case  
                    when isnull(lmd.BaseFileReceivedToSubmittedForAuditBusDays, 0) = 0 then '[0]'  
                    when lmd.BaseFileReceivedToSubmittedForAuditBusDays <= 1 then '(0 - 1]'  
                    when lmd.BaseFileReceivedToSubmittedForAuditBusDays <= 2 then '(1 - 2]'  
                    when lmd.BaseFileReceivedToSubmittedForAuditBusDays <= 3 then '(2 - 3]'  
                    when lmd.BaseFileReceivedToSubmittedForAuditBusDays <= 5 then '(3 - 5]'  
                    when lmd.BaseFileReceivedToSubmittedForAuditBusDays <= 10 then '(5 - 10]'  
                    when lmd.BaseFileReceivedToSubmittedForAuditBusDays <= 15 then '(10 - 15]'  
                    when lmd.BaseFileReceivedToSubmittedForAuditBusDays <= 20 then '(15 - 20]'  
                    when lmd.BaseFileReceivedToSubmittedForAuditBusDays <= 25 then '(20 - 25]'  
                    when lmd.BaseFileReceivedToSubmittedForAuditBusDays > 25 then '> 25'  
                    end,  
                BaseFileReceivedToPendingConditionsCalDays = isnull(lmd.BaseFileReceivedToPendingConditionsCalDays, 0),  
                BaseFileReceivedToPendingConditionsCalDaysGrp = case  
                    when isnull(lmd.BaseFileReceivedToPendingConditionsCalDays, 0) = 0 then '[0]'  
                    when lmd.BaseFileReceivedToPendingConditionsCalDays <= 1 then '(0 - 1]'  
                    when lmd.BaseFileReceivedToPendingConditionsCalDays <= 2 then '(1 - 2]'  
                    when lmd.BaseFileReceivedToPendingConditionsCalDays <= 3 then '(2 - 3]'  
                    when lmd.BaseFileReceivedToPendingConditionsCalDays <= 5 then '(3 - 5]'  
                    when lmd.BaseFileReceivedToPendingConditionsCalDays <= 10 then '(5 - 10]'  
                    when lmd.BaseFileReceivedToPendingConditionsCalDays <= 15 then '(10 - 15]'  
                    when lmd.BaseFileReceivedToPendingConditionsCalDays <= 20 then '(15 - 20]'  
                    when lmd.BaseFileReceivedToPendingConditionsCalDays <= 25 then '(20 - 25]'  
                    when lmd.BaseFileReceivedToPendingConditionsCalDays > 25 then '> 25'  
                    end,  
                BaseFileReceivedToPendingConditionsBusDays = isnull(lmd.BaseFileReceivedToPendingConditionsBusDays, 0),  
                BaseFileReceivedToPendingConditionsBusDaysGrp = case  
                    when isnull(lmd.BaseFileReceivedToPendingConditionsBusDays, 0) = 0 then '[0]'  
                    when lmd.BaseFileReceivedToPendingConditionsBusDays <= 1 then '(0 - 1]'  
                    when lmd.BaseFileReceivedToPendingConditionsBusDays <= 2 then '(1 - 2]'  
                    when lmd.BaseFileReceivedToPendingConditionsBusDays <= 3 then '(2 - 3]'  
                    when lmd.BaseFileReceivedToPendingConditionsBusDays <= 5 then '(3 - 5]'  
                    when lmd.BaseFileReceivedToPendingConditionsBusDays <= 10 then '(5 - 10]'  
                    when lmd.BaseFileReceivedToPendingConditionsBusDays <= 15 then '(10 - 15]'  
                    when lmd.BaseFileReceivedToPendingConditionsBusDays <= 20 then '(15 - 20]'  
                    when lmd.BaseFileReceivedToPendingConditionsBusDays <= 25 then '(20 - 25]'  
                    when lmd.BaseFileReceivedToPendingConditionsBusDays > 25 then '> 25'  
                    end,  
                PendingConditionsToSubmittedForFundingReviewCalDays = isnull(lmd.PendingConditionsToSubmittedForFundingReviewCalDays, 0),  
                PendingConditionsToSubmittedForFundingReviewCalDaysGrp = case  
                    when isnull(lmd.PendingConditionsToSubmittedForFundingReviewCalDays, 0) = 0 then '[0]'  
                    when lmd.PendingConditionsToSubmittedForFundingReviewCalDays <= 1 then '(0 - 1]'  
                    when lmd.PendingConditionsToSubmittedForFundingReviewCalDays <= 2 then '(1 - 2]'  
                    when lmd.PendingConditionsToSubmittedForFundingReviewCalDays <= 3 then '(2 - 3]'  
                    when lmd.PendingConditionsToSubmittedForFundingReviewCalDays <= 5 then '(3 - 5]'  
                    when lmd.PendingConditionsToSubmittedForFundingReviewCalDays <= 10 then '(5 - 10]'  
                    when lmd.PendingConditionsToSubmittedForFundingReviewCalDays <= 15 then '(10 - 15]'  
                    when lmd.PendingConditionsToSubmittedForFundingReviewCalDays <= 20 then '(15 - 20]'  
                    when lmd.PendingConditionsToSubmittedForFundingReviewCalDays <= 25 then '(20 - 25]'  
                    when lmd.PendingConditionsToSubmittedForFundingReviewCalDays > 25 then '> 25'  
                 end,  
                PendingConditionsToSubmittedForFundingReviewBusDays = isnull(lmd.PendingConditionsToSubmittedForFundingReviewBusDays, 0),  
                PendingConditionsToSubmittedForFundingReviewBusDaysGrp = case  
                    when isnull(lmd.PendingConditionsToSubmittedForFundingReviewBusDays, 0) = 0 then '[0]'  
                    when lmd.PendingConditionsToSubmittedForFundingReviewBusDays <= 1 then '(0 - 1]'  
                    when lmd.PendingConditionsToSubmittedForFundingReviewBusDays <= 2 then '(1 - 2]'  
                    when lmd.PendingConditionsToSubmittedForFundingReviewBusDays <= 3 then '(2 - 3]'  
                    when lmd.PendingConditionsToSubmittedForFundingReviewBusDays <= 5 then '(3 - 5]'  
                    when lmd.PendingConditionsToSubmittedForFundingReviewBusDays <= 10 then '(5 - 10]'  
                    when lmd.PendingConditionsToSubmittedForFundingReviewBusDays <= 15 then '(10 - 15]'  
                    when lmd.PendingConditionsToSubmittedForFundingReviewBusDays <= 20 then '(15 - 20]'  
                    when lmd.PendingConditionsToSubmittedForFundingReviewBusDays <= 25 then '(20 - 25]'  
                    when lmd.PendingConditionsToSubmittedForFundingReviewBusDays > 25 then '> 25'  
                    end,  
                SubmittedForFundingReviewToApprovedForPurchaseCalDays = isnull(lmd.SubmittedForFundingReviewToApprovedForPurchaseCalDays, 0),  
                SubmittedForFundingReviewToApprovedForPurchaseCalDaysGrp = case  
                    when isnull(lmd.SubmittedForFundingReviewToApprovedForPurchaseCalDays, 0) = 0 then '[0]'  
                    when lmd.SubmittedForFundingReviewToApprovedForPurchaseCalDays <= 1 then '(0 - 1]'  
                    when lmd.SubmittedForFundingReviewToApprovedForPurchaseCalDays <= 2 then '(1 - 2]'  
                    when lmd.SubmittedForFundingReviewToApprovedForPurchaseCalDays <= 3 then '(2 - 3]'  
                    when lmd.SubmittedForFundingReviewToApprovedForPurchaseCalDays <= 5 then '(3 - 5]'  
                    when lmd.SubmittedForFundingReviewToApprovedForPurchaseCalDays <= 10 then '(5 - 10]'  
                    when lmd.SubmittedForFundingReviewToApprovedForPurchaseCalDays <= 15 then '(10 - 15]'  
                    when lmd.SubmittedForFundingReviewToApprovedForPurchaseCalDays <= 20 then '(15 - 20]'  
                    when lmd.SubmittedForFundingReviewToApprovedForPurchaseCalDays <= 25 then '(20 - 25]'  
                    when lmd.SubmittedForFundingReviewToApprovedForPurchaseCalDays > 25 then '> 25'  
                    end,  
                SubmittedForFundingReviewToApprovedForPurchaseBusDays = isnull(lmd.SubmittedForFundingReviewToApprovedForPurchaseBusDays, 0),  
                SubmittedForFundingReviewToApprovedForPurchaseBusDaysGrp = case  
                    when isnull(lmd.SubmittedForFundingReviewToApprovedForPurchaseBusDays, 0) = 0 then '[0]'  
                    when lmd.SubmittedForFundingReviewToApprovedForPurchaseBusDays <= 1 then '(0 - 1]'  
                    when lmd.SubmittedForFundingReviewToApprovedForPurchaseBusDays <= 2 then '(1 - 2]'  
                    when lmd.SubmittedForFundingReviewToApprovedForPurchaseBusDays <= 3 then '(2 - 3]'  
                    when lmd.SubmittedForFundingReviewToApprovedForPurchaseBusDays <= 5 then '(3 - 5]'  
                    when lmd.SubmittedForFundingReviewToApprovedForPurchaseBusDays <= 10 then '(5 - 10]'  
                    when lmd.SubmittedForFundingReviewToApprovedForPurchaseBusDays <= 15 then '(10 - 15]'  
                    when lmd.SubmittedForFundingReviewToApprovedForPurchaseBusDays <= 20 then '(15 - 20]'  
                    when lmd.SubmittedForFundingReviewToApprovedForPurchaseBusDays <= 25 then '(20 - 25]'  
                    when lmd.SubmittedForFundingReviewToApprovedForPurchaseBusDays > 25 then '> 25'  
                    end,  
                BaseFileReceivedToApprovedForPurchaseCalDays = isnull(lmd.BaseFileReceivedToApprovedForPurchaseCalDays, 0),  
                BaseFileReceivedToApprovedForPurchaseCalDaysGrp = case  
                    when isnull(lmd.BaseFileReceivedToApprovedForPurchaseCalDays, 0) = 0 then '[0]'  
                    when lmd.BaseFileReceivedToApprovedForPurchaseCalDays <= 1 then '(0 - 1]'  
                    when lmd.BaseFileReceivedToApprovedForPurchaseCalDays <= 2 then '(1 - 2]'  
                    when lmd.BaseFileReceivedToApprovedForPurchaseCalDays <= 3 then '(2 - 3]'  
                    when lmd.BaseFileReceivedToApprovedForPurchaseCalDays <= 5 then '(3 - 5]'  
                    when lmd.BaseFileReceivedToApprovedForPurchaseCalDays <= 10 then '(5 - 10]'  
                    when lmd.BaseFileReceivedToApprovedForPurchaseCalDays <= 15 then '(10 - 15]'  
                    when lmd.BaseFileReceivedToApprovedForPurchaseCalDays <= 20 then '(15 - 20]'  
                    when lmd.BaseFileReceivedToApprovedForPurchaseCalDays <= 25 then '(20 - 25]'  
                    when lmd.BaseFileReceivedToApprovedForPurchaseCalDays > 25 then '> 25'  
                    end,  
                BaseFileReceivedToApprovedForPurchaseBusDays = isnull(lmd.BaseFileReceivedToApprovedForPurchaseBusDays, 0),  
                BaseFileReceivedToApprovedForPurchaseBusDaysGrp = case  
                    when isnull(lmd.BaseFileReceivedToApprovedForPurchaseBusDays, 0) = 0 then '[0]'  
                    when lmd.BaseFileReceivedToApprovedForPurchaseBusDays <= 1 then '(0 - 1]'  
                    when lmd.BaseFileReceivedToApprovedForPurchaseBusDays <= 2 then '(1 - 2]'  
                    when lmd.BaseFileReceivedToApprovedForPurchaseBusDays <= 3 then '(2 - 3]'  
                    when lmd.BaseFileReceivedToApprovedForPurchaseBusDays <= 5 then '(3 - 5]'  
                    when lmd.BaseFileReceivedToApprovedForPurchaseBusDays <= 10 then '(5 - 10]'  
                    when lmd.BaseFileReceivedToApprovedForPurchaseBusDays <= 15 then '(10 - 15]'  
                    when lmd.BaseFileReceivedToApprovedForPurchaseBusDays <= 20 then '(15 - 20]'  
                    when lmd.BaseFileReceivedToApprovedForPurchaseBusDays <= 25 then '(20 - 25]'  
                    when lmd.BaseFileReceivedToApprovedForPurchaseBusDays > 25 then '> 25'  
                    end,  
                nla.LoanAcqFedRefNumTxt,  
                lq.CreditScoreNum,  
                credit_rgs.CreditScoreRg,  
                lt.LoanAmt,  
                amt_rgs.LoanAmtRg,  
                lq.LtvRatio,  
                ltv_rgs.LtvRg,  
                lq.TotalDebtExpenseRatio,  
                dti_rgs.TotalDebtExpenseRatioRg,  
    lq.CltvRatio,  
    cltv_rgs.CltvRg,  
    la.LoanAcqNoteRate,  
                FixedNumeratorLoanAmt = case when at.AmortTypeName = 'Fixed' then la.LoanAcqPrinBalAmt else 0 end,  
                FixedDenominatorLoanAmt = la.LoanAcqPrinBalAmt,  
                ArmNumeratorLoanAmt = case when at.AmortTypeName = 'Arm' then la.LoanAcqPrinBalAmt else 0 end,  
                ArmDenominatorLoanAmt = la.LoanAcqPrinBalAmt,  
                ConfNumeratorLoanAmt = case when mt.MtgTypeName = 'Conf' then la.LoanAcqPrinBalAmt else 0 end,  
                ConfDenominatorLoanAmt = la.LoanAcqPrinBalAmt,  
                FhaNumeratorLoanAmt = case when mt.MtgTypeName = 'Fha' then la.LoanAcqPrinBalAmt else 0 end,  
                FhaDenominatorLoanAmt = la.LoanAcqPrinBalAmt,  
                VaNumeratorLoanAmt = case when mt.MtgTypeName = 'Va' then la.LoanAcqPrinBalAmt else 0 end,  
                VaDenominatorLoanAmt = la.LoanAcqPrinBalAmt,  
                NonConfNumeratorLoanAmt = case when mt.MtgTypeName = 'NonConf' then la.LoanAcqPrinBalAmt else 0 end,  
                NonConfDenominatorLoanAmt = la.LoanAcqPrinBalAmt,  
                RefiNumeratorLoanAmt = case when lt.LoanPurposeTypeId = 2 then la.LoanAcqPrinBalAmt else 0 end,  
                RefiDenominatorLoanAmt = la.LoanAcqPrinBalAmt,  
                NoStipBit = convert(bit, case  
                    when (lsd.LoanId is null or lsd.ClientMetricsTotalEverStipCnt = 0) then 1   --For Encompass LoanCondition, it could have ZERO records, so has to check for NULL on left join  
                    else 0  
                    end),  
                ClientMetricsTotalEverStipCnt = isnull(lsd.ClientMetricsTotalEverStipCnt,0),  
                ClientMetricsTotalEverStipCntGrp = case  
     when lsd.LoanId is null then '0'  
                    when lsd.ClientMetricsTotalEverStipCnt = 0 then '0'  
                    when lsd.ClientMetricsTotalEverStipCnt = 1 then '1'  
                    when lsd.ClientMetricsTotalEverStipCnt = 2 then '2'  
                    when lsd.ClientMetricsTotalEverStipCnt between 3 and 5 then '3 - 5'  
                    when lsd.ClientMetricsTotalEverStipCnt between 6 and 10 then '6 - 10'  
                    when lsd.ClientMetricsTotalEverStipCnt between 11 and 15 then '11 - 15'  
                    when lsd.ClientMetricsTotalEverStipCnt > 15 then '> 15'  
                    end,  
                NoteRateNumeratorAcqPrinBalAmt = la.LoanAcqNoteRate * la.LoanAcqPrinBalAmt,  
                NoteRateDenominatorAcqPrinBalAmt = case when la.LoanAcqNoteRate is not null then la.LoanAcqPrinBalAmt end,  
                LtvRatioNumeratorAcqPrinBalAmt = lq.LtvRatio * la.LoanAcqPrinBalAmt,  
                LtvRatioDenominatorAcqPrinBalAmt = case when lq.LtvRatio is not null then la.LoanAcqPrinBalAmt end,  
                CltvRatioNumeratorAcqPrinBalAmt = lq.CltvRatio * la.LoanAcqPrinBalAmt,  
                CltvRatioDenominatorAcqPrinBalAmt = case when lq.CltvRatio is not null then la.LoanAcqPrinBalAmt end,  
                CreditScoreNumeratorAcqPrinBalAmt = lq.CreditScoreNum * la.LoanAcqPrinBalAmt,  
                CreditScoreDenominatorAcqPrinBalAmt = case when lq.CreditScoreNum is not null then la.LoanAcqPrinBalAmt end,  
                TotalDebtExpenseRatioNumeratorAcqPrinBalAmt = lq.TotalDebtExpenseRatio * la.LoanAcqPrinBalAmt,  
                TotalDebtExpenseRatioDenominatorAcqPrinBalAmt = case when lq.TotalDebtExpenseRatio is not null then la.LoanAcqPrinBalAmt end  
            from Loan l  
            join LoanFund lf  
                on l.LoanId = lf.LoanId  
            left join EzLoanAcq la  
               on l.LoanId = la.LoanId  
            left join EzLoanLock ll  
                on l.LoanId = ll.LoanId  
            left join npi.LoanAcq nla  
                on l.LoanId = nla.LoanId  
            left join EzLoanChannel lc  
                on l.LoanId = lc.LoanId  
            left join EzLoanUnderwriting lu  
                on l.LoanId = lu.LoanId  
            left join LoanPropAddr lpa  
                on l.LoanId = lpa.LoanId  
            left join LoanMilestoneDtl_All lmd  
                on l.LoanId = lmd.LoanId  
            left join LoanQualification lq  
                on l.LoanId = lq.LoanId  
            left join EzLoanTerms lt  
                on l.LoanId = lt.LoanId  
            left join AmortType at  
                on lt.AmortTypeId = at.AmortTypeId  
            left join MtgType mt  
                on lt.MtgTypeId = mt.MtgTypeId  
   left join EzLoanProp lp  
    on l.LoanId = lp.LoanId  
            left join LoanStipDtl_All lsd  -- CTE  
                on l.LoanId = lsd.LoanId  
   left join EzLoanResponsibility lr  
          on l.LoanId = lr.LoanId  
     and lr.ResponsibilityTypeId = 1  
   left join npi.LoanBor plb  
    on l.LoanId = plb.LoanId  
     and plb.BorRefNum = 1  
   left join Seller ezslr  
    on lc.SellerPartyId = ezslr.SellerPartyId  
   left join EzPerson ezp  
    on ezp.PersonId = ezslr.SellerSalesExecPersonId  
            outer apply CreditScoreRgs (lq.CreditScoreNum) credit_rgs  
   outer apply LoanAmtRgs (lt.LoanAmt) amt_rgs  
            outer apply LtvRgs (lq.LtvRatio) ltv_rgs  
   outer apply CltvRgs (lq.CltvRatio) cltv_rgs  
            outer apply TotalDebtExpenseRatioRgs (lq.TotalDebtExpenseRatio) dti_rgs  
            cross apply Days(ll.InitialLockDt, la.LoanAcqDt, 'B') LockedToFundBusDays  
            cross apply Days(ll.InitialLockDt, lmd.BaseFileReceivedDtTm, 'B') LockedToDelivery  
            where lf.FundedBit = 1),  
    b as (  
        select *,  
            LockedToFundedCalDaysGrp = convert(varchar(32), case  
                when LockedToFundedCalDays <= 5 then '[0 - 5]'  
                when LockedToFundedCalDays <= 10 then '(5 - 10]'  
              when LockedToFundedCalDays <= 15 then '(10 - 15]'  
                when LockedToFundedCalDays <= 20 then '(15 - 20]'  
                when LockedToFundedCalDays <= 30 then '(20 - 30]'  
                when LockedToFundedCalDays <= 45 then '(30 - 45]'  
                when LockedToFundedCalDays <= 60 then '(45 - 60]'  
                when LockedToFundedCalDays > 60 then '> 60'  
                end),  
            LockedToFundedBusDaysGrp = convert(varchar(32), case  
                when LockedToFundedBusDays is null then 'Unknown'  
                when LockedToFundedBusDays <= 5 then '[0 - 5]'  
                when LockedToFundedBusDays <= 10 then '(5 - 10]'  
                when LockedToFundedBusDays <= 15 then '(10 - 15]'  
                when LockedToFundedBusDays <= 20 then '(15 - 20]'  
                when LockedToFundedBusDays <= 30 then '(20 - 30]'  
                when LockedToFundedBusDays <= 45 then '(30 - 45]'  
                when LockedToFundedBusDays <= 60 then '(45 - 60]'  
                when LockedToFundedBusDays > 60 then '> 60'  
                end),  
            LockedToBaseFileReceivedCalDaysGrp = convert(varchar(32), case  
           when LockedToBaseFileReceivedCalDays <= 5 then '[0 - 5]'  
                when LockedToBaseFileReceivedCalDays <= 10 then '(5 - 10]'  
                when LockedToBaseFileReceivedCalDays <= 15 then '(10 - 15]'  
                when LockedToBaseFileReceivedCalDays <= 20 then '(15 - 20]'  
                when LockedToBaseFileReceivedCalDays <= 30 then '(20 - 30]'  
                when LockedToBaseFileReceivedCalDays <= 45 then '(30 - 45]'  
                when LockedToBaseFileReceivedCalDays <= 60 then '(45 - 60]'  
                when LockedToBaseFileReceivedCalDays > 60 then '> 60'  
                end),  
            LockedToBaseFileReceivedBusDaysGrp = convert(varchar(32), case  
                when LockedToBaseFileReceivedBusDays is null then 'Unknown'  
                when LockedToBaseFileReceivedBusDays <= 5 then '[0 - 5]'  
                when LockedToBaseFileReceivedBusDays <= 10 then '(5 - 10]'  
                when LockedToBaseFileReceivedBusDays <= 15 then '(10 - 15]'  
                when LockedToBaseFileReceivedBusDays <= 20 then '(15 - 20]'  
                when LockedToBaseFileReceivedBusDays <= 30 then '(20 - 30]'  
                when LockedToBaseFileReceivedBusDays <= 45 then '(30 - 45]'  
                when LockedToBaseFileReceivedBusDays <= 60 then '(45 - 60]'  
                when LockedToBaseFileReceivedBusDays > 60 then '> 60'  
                end),  
            FundingAtReleaseBit = convert(bit, case  
                when NoStipBit = 1  
                    or PendingConditionsReachedBit = 0 then 1  
                else 0  
                end)  
                  
        from a)  
    select *,  
        FundingAtReleasePct = convert(float, FundingAtReleaseBit)  
    from b;  
  
  
  
  
  