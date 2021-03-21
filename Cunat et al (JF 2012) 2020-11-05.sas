proc datasets lib=work kill nolist memtype=data;
quit;

libname CF 'C:\Users\KUTHURU\Downloads\Laptop\Semester 3\Corporate Finance Slava\Homework-5'; run;

%let wrds=wrds-cloud.wharton.upenn.edu 4016;
options comamid=TCP remote=WRDS;
signon username=adarshkp password="{SAS002}17CB392C3171814A0F4275CF33F4601116D0FB3207889EC521ABAA62";              
                                         
libname rwork slibref = work server = wrds; run;

rsubmit;
options nocenter nodate nonumber ls=max ps=max msglevel=i; 
libname mf '/wrds/crsp/sasdata/q_mutualfunds'; run; *refers to crsp;
libname crspa '/wrds/crsp/sasdata/a_stock'; run; *refers to crsp;
libname ff '/wrds/ff/sasdata'; run;
libname s12 '/wrds/tfn/sasdata/s12'; run; *refers to tfn;
libname mfl '/wrds/mfl/sasdata'; run;
libname a_ccm '/wrds/crsp/sasdata/a_ccm'; run; *refers to crsp;
libname compa '/wrds/comp/sasdata/naa'; run; *refers to compa;
libname cmpny '/wrds/comp/sasdata/d_na/company'; run; *refers to company file;
libname pn '/wrds/comp/sasdata/d_na/pension'; run; *compustat pension;
libname temp '/scratch/bc/Adarsh'; run;

endrsubmit;


/*****************************************************************/
/* 			Step 1.  Import stata dataset and clean it;  		 */
/*****************************************************************/

proc import datafile="C:\Users\KUTHURU\Downloads\Laptop\Semester 3\Corporate Finance Slava\Homework-5\ISS - Voting Analytics - Company Vote Results US.dta" out=data dbms = dta replace;
run;
*427527 obs;

*include only pass/fail results and add a dummy if pass or fail;
data data;
	set data;
	if base='NA' then delete;
	if missing(voteresult)= 1 then delete;
	if voteresult in ('Pass','Fail');
	format meetingdate date9.;
run;
*364615 obs;

*drop director elections and estimate vote for ratio;
data data;
	set data;
	if agendageneraldesc='Elect Director' then delete;
	for=votedfor/sum(votedfor,votedAgainst);
	if missing(for)=1 then delete;
run;
*106414 obs;

*drop observations with vote requirement=0.01 and if for>vote requirement and did not pass;
data data;
	set data;
	if voterequirement=0.5;
	if for>voterequirement and voteresult='Fail' then delete;
	if for<voterequirement and voteresult='Pass' then delete;
run;
*100804 obs;	 

*add pass/fail dummy;
data data;
	set data;
	if voteresult='Pass' then pass=1;
	else pass=0;
run;
*among 100804 obs, 7064 obs are fail, rest are pass;

*Add 2-month before date and 200 days date;
data data;
	set data;
	date_2month=intnx("month",meetingdate,-2);
	date200=intnx("day",date_2month,200);
	format date_2month date9. date200 date9.;
run;

/*****************************************************************/
/* 	            Step 1. Obtain CRSP return data                  */
/*****************************************************************/

rsubmit;
proc upload data=data out=data; run;

/* Step 1.1 Merge crsp data with linktable */
proc sort data=crspa.stocknames out=crsp;
	by permno nameenddt;
run;
proc sql;
	create table crsp_m as
	select a.*, b.ticker
	from crspa.dsf(keep=permno permco cusip prc shrout date ret retx) as a, crsp as b
	where a.permno = b.permno 
	order by a.permco, a.permno, a.date;
quit;

proc sql;
create table crsp_ccm as
select a.*, b.gvkey, b.linkprim, b.linktype
from crsp_m as a, a_ccm.ccmxpf_lnkhist as b
where a.permno=b.lpermno and a.permco = b.lpermco
	and (b.LINKDT <= intnx('month',a.date,0,'E') ) 
	and (intnx('month',a.date,0,'B') <= b.LINKENDDT or missing(b.LINKENDDT) )
	and b.lpermno ^=. and b.LINKTYPE in ("LU","LC")
order by a.permno, a.date;
quit;

/* Step 1.2 Merge crsp data with voting data*/
proc sql;
	create table data1 as
	select distinct a.*,b.*
	from data as a left join crsp_ccm as b
	on a.ticker=b.ticker and a.date_2month<=b.date<a.date200
	order by a.ticker,a.meetingdate,b.date;
quit;

proc sort data=data1 nodupkey out=data2;by ticker meetingdate date; run;
*5,934,967 obs;


/************************************/
/*Step 2. Extract FF-3 + MOM factors*/
/************************************/
proc sql;
	create table data3 as
	select distinct a.*,b.*
	from data2 as a left join ff.factors_daily as b
	on a.date=b.date;
quit;
*5,934,967 obs;

proc download data=data3 out=data3; run;

endrsubmit;


*estimate excess return;
data data3;
	set data3;
	ExRet=sum(ret,-rf);
run;

/* Step 2.1 Estimate the coefficients from fama-french regression */

**Coefficients;
proc sort data=data3; by ticker meetingdate; run;
proc reg data=data3 noprint tableout outest=Coeff;
  by ticker meetingdate;
  model ExRet = MKTRF SMB HML UMD;
quit;
data Coeff;
  set Coeff;
  where _Type_ in ('PARMS');
  keep ticker meetingdate _Type_ MKTRF SMB HML UMD;
  if missing(ticker)=0;
  if _Type_='PARMS';
  rename mktrf=beta1;
  rename smb=beta2;
  rename hml=beta3;
  rename umd=beta4;
run;

/* Step 2.2 Merge coefficients data with voting dataset to estimate abnormal returns (alphas)*/
data data4;
	set data3;
	if meetingdate=date;
run;
*42973 obs;

proc sql;
	create table data5 as
	select distinct a.*,b.*
	from data4 as a left join coeff as b
	on a.ticker=b.ticker and a.meetingdate=b.meetingdate;
quit;
*42973 obs;

*Step 2.2.1. calculate expected returns and abnormal returns (alphas);
data data5;
	set data5;
	expected_ret=sum(MKTRF*beta1, SMB*beta2, HML*beta3, UMD*beta4);
	alpha1=sum(ret,-expected_ret);
	year=year(meetingdate);
run;

*Step 2.2.2. add percentage windows;
data data5;
	set data5;
	if for>=0.45 and for<=0.55 then five=1;
	else five=0;
	if for>=0.40 and for<=0.60 then ten=1;
	else ten=0;
	if for>=0.48 and for<=0.52 then two=1;
	else two=0;
	if for>=0.49 and for<=0.51 then one=1;
	else one=0;
run;

data test;
	set data5;
	if five=1;
run;

*Export csv;
proc export data=data5 outfile="C:\Users\KUTHURU\Downloads\Laptop\Semester 3\Corporate Finance Slava\Homework-5\data.csv" 
dbms=csv replace;
run;
