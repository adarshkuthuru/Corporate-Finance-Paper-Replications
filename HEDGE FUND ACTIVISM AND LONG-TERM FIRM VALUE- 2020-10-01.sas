proc datasets lib=work kill nolist memtype=data;
quit;

libname CF 'C:\Users\KUTHURU\Downloads\Laptop\Semester 3\Corporate Finance Slava\Homework-3'; run;



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
libname compg '/wrds/comp/sasdata/d_global'; run; *refers to compg;
libname pn '/wrds/comp/sasdata/naa/pension'; run; *compustat pension;
libname temp '/scratch/bc/Adarsh'; run;

endrsubmit;



/***************************************************/
/*Step 1. Obtain data and summary stats: Table 1B  */
/***************************************************/

rsubmit;
*Specify CRSP begin and end dates;
%let crspbegdate = '01Jan1995'd;
%let crspenddate = '31DEC2011'd; 
/* Step 1.1. Obtain Data from Compustat */
data comp_extract;
   format gvkey datadate date_fyend calyear fyear fyr indfmt consol datafmt popsrc sich dlc dltt at prcc_f csho
	ceq capx oibdp ppent xrd;  /*http://www.crsp.com/products/documentation/annual-data-industrial */
   set compa.funda
   (where=(fyr>0 and at>0 and consol='C' and
           indfmt='INDL' and datafmt='STD' and popsrc='D'));

   if missing(at)=0;
   if year(&crspbegdate)<=year(datadate)<=year(&crspenddate);
   format datadate date9.;
   keep gvkey datadate calyear fyear fyr indfmt consol datafmt popsrc sich dlc dltt at prcc_f csho
	ceq capx oibdp ppent xrd;
run;
*171725 obs; 

/* Step 1.2. Estimate variables */
data comp_extract;
	set comp_extract;
	TobinQ=sum(at,-ceq,prcc_f*csho)/at; *at, ceq, csho in millions;
	Lnsize=log(at); *at in millions;
	Leverage=sum(dltt,dlc)/at; *dltt, dlc, at in millions;
	CAPX=capx/at;
	Intangibility=sum(1,-(ppent/at)); *ppen, at in millions;
	ROA=oibdp/at; *oibdp, at in millions;
	Lnmarketvalue=log(abs(prcc_f)*csho); *csho in millions;
	RD=xrd/at; *xrd, at in millions;
	calyear=year(datadate);
	if sich >5999 and sich <7000 then delete; *exclude financial firms;
run; 
*147826 obs; 

*Add industry median TobinQ to the main dataset;
proc sql;
	create table comp_extract1 as
	select distinct calyear, sich, median(TobinQ) as IndMedian
	from comp_extract
	group by calyear, sich;
quit;

proc sql;
	create table comp_extract as
	select distinct a.*, b.IndMedian
	from comp_extract as a left join comp_extract1 as b
	on a.calyear=b.calyear and a.sich=b.sich;
quit;

*Industry adjusted TobinQ;
data comp_extract;
	set comp_extract;
	TobinQ1=sum(TobinQ,-IndMedian);
run;
*147826 obs;  

/* Step 1.3. Winsorize the variables*/
%macro winsor(dsetin=, dsetout=, byvar=none, vars=, type=winsor, pctl=1 99);
  
%if &dsetout = %then %let dsetout = &dsetin;
     
%let varL=;
%let varH=;
%let xn=1;
  
%do %until ( %scan(&vars,&xn)= );
    %let token = %scan(&vars,&xn);
    %let varL = &varL &token.L;
    %let varH = &varH &token.H;
    %let xn=%EVAL(&xn + 1);
%end;
  
%let xn=%eval(&xn-1);
  
data xtemp;
    set &dsetin;
    run;
  
%if &byvar = none %then %do;
  
    data xtemp;
        set xtemp;
        xbyvar = 1;
        run;
  
    %let byvar = xbyvar;
  
%end;
  
proc sort data = xtemp;
    by &byvar;
    run;
  
proc univariate data = xtemp noprint;
    by &byvar;
    var &vars;
    output out = xtemp_pctl PCTLPTS = &pctl PCTLPRE = &vars PCTLNAME = L H;
    run;
  
data &dsetout;
    merge xtemp xtemp_pctl;
    by &byvar;
    array trimvars{&xn} &vars;
    array trimvarl{&xn} &varL;
    array trimvarh{&xn} &varH;
  
    do xi = 1 to dim(trimvars);
  
        %if &type = winsor %then %do;
            if not missing(trimvars{xi}) then do;
              if (trimvars{xi} < trimvarl{xi}) then trimvars{xi} = trimvarl{xi};
              if (trimvars{xi} > trimvarh{xi}) then trimvars{xi} = trimvarh{xi};
            end;
        %end;
  
        %else %do;
            if not missing(trimvars{xi}) then do;
              if (trimvars{xi} < trimvarl{xi}) then delete;
              if (trimvars{xi} > trimvarh{xi}) then delete;
            end;
        %end;
  
    end;
    drop &varL &varH xbyvar xi;
    run;
  
%mend winsor;

/* invoke macro to winsorize */
%winsor(dsetin=comp_extract, dsetout=comp_extract, byvar=none, 
vars=TobinQ1 Lnsize Leverage CAPX Intangibility ROA Lnmarketvalue RD, type=winsor, pctl=1 99);


/* Step 1.3. Desciptive stats */
PROC UNIVARIATE DATA=comp_extract noprint;
  VAR TobinQ1;
  output out = temp
	pctlpts = 25 to 75 by 50 pctlpre = p_
	mean=mean std=stddev n=n;
RUN; 
data temp; format stat; set temp; stat='TobinQ'; run;

PROC UNIVARIATE DATA=comp_extract noprint;
  VAR Lnsize;
  output out = temp1
	pctlpts = 25 to 75 by 50 pctlpre = p_
	mean=mean std=stddev n=n;
RUN; 
data temp1; format stat; set temp1; stat='Log size'; run;

PROC UNIVARIATE DATA=comp_extract noprint;
  VAR Leverage;
  output out = temp2
	pctlpts = 25 to 75 by 50 pctlpre = p_
	mean=mean std=stddev n=n;
RUN; 
data temp2; format stat; set temp2; stat='Leverage'; run;

PROC UNIVARIATE DATA=comp_extract noprint;
  VAR CAPX;
  output out = temp3
	pctlpts = 25 to 75 by 50 pctlpre = p_
	mean=mean std=stddev n=n;
RUN; 
data temp3; format stat; set temp3; stat='CAPX'; run;

PROC UNIVARIATE DATA=comp_extract noprint;
  VAR Intangibility;
  output out = temp5
	pctlpts = 25 to 75 by 50 pctlpre = p_
	mean=mean std=stddev n=n;
RUN; 
data temp5; format stat; set temp5; stat='Intangibility'; run;

PROC UNIVARIATE DATA=comp_extract noprint;
  VAR ROA;
  output out = temp6
	pctlpts = 25 to 75 by 50 pctlpre = p_
	mean=mean std=stddev n=n;
RUN; 
data temp6; format stat; set temp6; stat='ROA'; run;

PROC UNIVARIATE DATA=comp_extract noprint;
  VAR Lnmarketvalue;
  output out = temp7
	pctlpts = 25 to 75 by 50 pctlpre = p_
	mean=mean std=stddev n=n;
RUN; 
data temp7; format stat; set temp7; stat='Log Mkt Equity'; run;

PROC UNIVARIATE DATA=comp_extract noprint;
  VAR RD;
  output out = temp4
	pctlpts = 25 to 75 by 50 pctlpre = p_
	mean=mean std=stddev n=n;
RUN; 
data temp4; format stat; set temp4; stat='R and D'; run;

proc append data=temp base=result; run;
proc append data=temp1 base=result force; run;
proc append data=temp2 base=result force; run;
proc append data=temp3 base=result force; run;
proc append data=temp4 base=result force; run;
proc append data=temp5 base=result force; run;
proc append data=temp6 base=result force; run;
proc append data=temp7 base=result force; run;

*drop tables;
proc sql;
drop table temp, temp1, temp2, temp3, temp4, temp5, temp6, temp7;
quit;

proc download data=result out=result; run;
endrsubmit;


*Export csv;
proc export data=result outfile="C:\Users\KUTHURU\Downloads\Laptop\Semester 3\Corporate Finance Slava\Homework-3\Table1b.csv" 
dbms=csv replace;
run;





/***************************************************/
/*Step 2. Obtain data and summary stats: Table 4A  */
/***************************************************/

rsubmit;
*Specify CRSP begin and end dates;
%let crspbegdate = '01Jan1989'd;
%let crspenddate = '31DEC2017'd; 
/* Step 2.1. Obtain Data from Compustat */
data comp_extract;
   format gvkey datadate date_fyend calyear fyear fyr indfmt consol datafmt popsrc sich dlc dltt at prcc_f csho
	ceq capx oibdp ppent xrd sale;  /*http://www.crsp.com/products/documentation/annual-data-industrial */
   set compa.funda
   (where=(fyr>0 and at>0 and consol='C' and
           indfmt='INDL' and datafmt='STD' and popsrc='D'));

   if missing(at)=0;
   if year(&crspbegdate)<=year(datadate)<=year(&crspenddate);
   format datadate date9.;
   keep gvkey datadate calyear fyear fyr indfmt consol datafmt popsrc sich dlc dltt at prcc_f csho
	ceq capx oibdp ppent xrd sale;
run;
*171725 obs; 

/* Step 2.2. Estimate variables */
data comp_extract;
	set comp_extract;
	TobinQ=sum(at,-ceq,prcc_f*csho)/at; *at, ceq, csho in millions;
	Lnsize=log(at); *at in millions;
	Leverage=sum(dltt,dlc)/at; *dltt, dlc, at in millions;
	CAPX=capx/at;
	Intangibility=sum(1,-(ppent/at)); *ppen, at in millions;
	ROA=oibdp/at; *oibdp, at in millions;
	Lnmarketvalue=log(abs(prcc_f)*csho); *csho in millions;
	RD=xrd/at; *xrd, at in millions;
	calyear=year(datadate);
	Lnsale=log(sale); *sale in millions;
	if sich >5999 and sich <7000 then delete; *exclude financial firms;
run; 
*147826 obs; 

*Add industry median TobinQ to the main dataset;
proc sql;
	create table comp_extract1 as
	select distinct calyear, sich, median(TobinQ) as IndMedian
	from comp_extract
	group by calyear, sich;
quit;

proc sql;
	create table comp_extract as
	select distinct a.*, b.IndMedian
	from comp_extract as a left join comp_extract1 as b
	on a.calyear=b.calyear and a.sich=b.sich;
quit;

*Industry adjusted TobinQ;
data comp_extract;
	set comp_extract;
	TobinQ1=sum(TobinQ,-IndMedian);
run;
*147826 obs;  


proc download data=comp_extract out=comp_extract1; run;
endrsubmit;

/* Step 2.3. Import targeted firms dataset and append it to fundamentals from Compustat*/

***Import xlsx;
PROC IMPORT OUT= HFdata
            DATAFILE= "C:\Users\KUTHURU\Downloads\Laptop\Semester 3\Corporate Finance Slava\Homework-3\HFA 1994-2014 MFIN 8895.xlsx" 
            DBMS=EXCEL REPLACE;
sheet="Sheet1";
GETNAMES=YES;
MIXED=NO;
SCANTEXT=YES;
USEDATE=YES;
SCANTIME=YES;
RUN;
*4058 obs;

*convert numeric date format;
data HFdata;
	set HFdata;
	newdate = input(put(date13d,8.),yymmdd8.);
	calyear = year(newdate);
	if missing(newdate)=1 then delete;
	format newdate date9.;
run;
*3645 obs;

*converts character variables to numbers;
data comp_extract1;
	set comp_extract1;
	gvkey1 = input(gvkey,12.);
run;

data HFdata;
	set HFdata;
	gvkey1 = input(gvkey,12.);
	if gvkey1=-99 or missing(gvkey1)=1 then delete;
run;
*3480 obs;

*check if multiple funds target same firm in one year;
proc sort data=hfdata nodupkey; by gvkey1 calyear; run;
*3240 obs;
proc sort data=comp_extract1 nodupkey; by gvkey1 calyear; run;
*236,244 obs;

*merging the targeted firms dataset with fundamentals;
proc sql;
	create table comp_extract2 as
	select distinct a.*, b.newdate as targetdate
	from comp_extract1 as a left join hfdata as b
	on a.calyear=b.calyear and a.gvkey1=b.gvkey1;
quit;
*236,244 obs;

*number of firms matched to original dataset;
data test;
	set comp_extract2;
	if missing(targetdate)=0;
run;
*2606 obs matched to entire sample;

*create target dummy;
data comp_extract2;
	set comp_extract2;
	if missing(targetdate)=0 then target=1;
	else target=0;
run;

/* Step 2.4. Add the lag variables required for Abadie-Imbens matching */

**TobinQ lags;
proc sql;
	create table comp_extract2 as
	select distinct a.*, b.tobinq1 as tobinq_1
	from comp_extract2 as a left join comp_extract2 as b
	on a.gvkey=b.gvkey and a.calyear=b.calyear+1;
quit;
proc sql;
	create table comp_extract2 as
	select distinct a.*, b.tobinq1 as tobinq_2
	from comp_extract2 as a left join comp_extract2 as b
	on a.gvkey=b.gvkey and a.calyear=b.calyear+2;
quit;
proc sql;
	create table comp_extract2 as
	select distinct a.*, b.tobinq1 as tobinq_3
	from comp_extract2 as a left join comp_extract2 as b
	on a.gvkey=b.gvkey and a.calyear=b.calyear+3;
quit;
proc sql;
	create table comp_extract2 as
	select distinct a.*, b.tobinq1 as tobinq_4
	from comp_extract2 as a left join comp_extract2 as b
	on a.gvkey=b.gvkey and a.calyear=b.calyear+4;
quit;
proc sql;
	create table comp_extract2 as
	select distinct a.*, b.tobinq1 as tobinq_5
	from comp_extract2 as a left join comp_extract2 as b
	on a.gvkey=b.gvkey and a.calyear=b.calyear+5;
quit;

**TobinQ leads;
proc sql;
	create table comp_extract2 as
	select distinct a.*, b.tobinq1
	from comp_extract2 as a left join comp_extract2 as b
	on a.gvkey=b.gvkey and a.calyear=b.calyear-1;
quit;
proc sql;
	create table comp_extract2 as
	select distinct a.*, b.tobinq1 as tobinq2
	from comp_extract2 as a left join comp_extract2 as b
	on a.gvkey=b.gvkey and a.calyear=b.calyear-2;
quit;
proc sql;
	create table comp_extract2 as
	select distinct a.*, b.tobinq1 as tobinq3
	from comp_extract2 as a left join comp_extract2 as b
	on a.gvkey=b.gvkey and a.calyear=b.calyear-3;
quit;
proc sql;
	create table comp_extract2 as
	select distinct a.*, b.tobinq1 as tobinq4
	from comp_extract2 as a left join comp_extract2 as b
	on a.gvkey=b.gvkey and a.calyear=b.calyear-4;
quit;
proc sql;
	create table comp_extract2 as
	select distinct a.*, b.tobinq1 as tobinq5
	from comp_extract2 as a left join comp_extract2 as b
	on a.gvkey=b.gvkey and a.calyear=b.calyear-5;
quit;

**Lags of other covariates;
proc sql;
	create table comp_extract2 as
	select distinct a.*, b.Lnmarketvalue as Lnmarketvalue_1
	from comp_extract2 as a left join comp_extract2 as b
	on a.gvkey=b.gvkey and a.calyear=b.calyear+1;
quit;
proc sql;
	create table comp_extract2 as
	select distinct a.*, b.Leverage as Leverage_1
	from comp_extract2 as a left join comp_extract2 as b
	on a.gvkey=b.gvkey and a.calyear=b.calyear+1;
quit;
proc sql;
	create table comp_extract2 as
	select distinct a.*, b.roa as roa_1
	from comp_extract2 as a left join comp_extract2 as b
	on a.gvkey=b.gvkey and a.calyear=b.calyear+1;
quit;
proc sql;
	create table comp_extract2 as
	select distinct a.*, b.Lnsize as Lnsize_1
	from comp_extract2 as a left join comp_extract2 as b
	on a.gvkey=b.gvkey and a.calyear=b.calyear+1;
quit;
proc sql;
	create table comp_extract2 as
	select distinct a.*, b.capx as capx_1
	from comp_extract2 as a left join comp_extract2 as b
	on a.gvkey=b.gvkey and a.calyear=b.calyear+1;
quit;
proc sql;
	create table comp_extract2 as
	select distinct a.*, b.rd as rd_1
	from comp_extract2 as a left join comp_extract2 as b
	on a.gvkey=b.gvkey and a.calyear=b.calyear+1;
quit;
proc sql;
	create table comp_extract2 as
	select distinct a.*, b.Intangibility as Intangibility_1
	from comp_extract2 as a left join comp_extract2 as b
	on a.gvkey=b.gvkey and a.calyear=b.calyear+1;
quit;
proc sql;
	create table comp_extract2 as
	select distinct a.*, b.Lnsale as Lnsale_1
	from comp_extract2 as a left join comp_extract2 as b
	on a.gvkey=b.gvkey and a.calyear=b.calyear+1;
quit;
*236244 obs;

**sample for analysis;
data comp_extract2;
	set comp_extract2;
	if calyear<1994 or calyear>2011 then delete;
run;
*156347 obs;

/* Step 2.5. Abadie-Imbens matching: the year before Hedge Funds file 13D */

%macro ps_matching(data=,y=,w=,x=,M=,Link=,L=,Lt=);

/** ps_matching estimates the Average Treatment Effect (ATE) and 
    the Average Treatment Effect for the Treated (ATET) 
    based on propensity scores with nearest-neighbor matching with replacement.
    Author : Nicolas Moreau, Université de La Réunion, CEMOI, October 2016
             Revised version: April 2017
**/

data table;
set &data(keep=gvkey &y &x &w);
_id_+1;
run; 

/*** Propensity score estimation ***/

%if %upcase(&Link)=PROBIT %then %let estim_method=PROBIT;%else %let estim_method=LOGIT;

title "Propensity Score Estimation";
proc logistic descending data=table;
model &w = &x/noint Link=&estim_method;
output out=pscore PREDICTED =pscore_pred xbeta=xbeta;
run;
title;

data pscore;
set pscore;
%if %upcase(&Link)=PROBIT %then %do;
BigF=probnorm(xbeta);  /* Probability F(xbeta) */
SmallF=pdf('normal',xbeta,0,1); /* Density function */ 
%end;
%else %do;
 BigF=exp(xbeta)/(1+exp(xbeta));      /* Probability F(xbeta) */
 Smallf=exp(xbeta)/(1+exp(xbeta))**2; /* Density function */ 
%end;
run;

/** File with propensity scores for treated **/

data treatment;
set pscore(where=(&w=1) rename=(gvkey=gvkeyT pscore_pred=pscoreT &y=outcomeT _id_=idT));

/** File with propensity scores for controls **/

data control;
set pscore(where=(&w=0) rename=(gvkey=gvkeyC pscore_pred=pscoreC  &y=outcomeC _id_=idC));
run;

/* Measuring distance between treated units and control units */

data DistanceT(rename=(gvkeyT=gvkey idT=_id_ outcomeT=outcome outcomeC=outcomeM idc=idM));
set Treatment(keep=gvkeyT idT pscoreT outcomeT);
do i= 1 to Ncontrol;
 set Control(keep=gvkeyC idC pscoreC outcomeC) point=i nobs=Ncontrol;
 Distance=abs(pscoreT-pscoreC);
 output;
end;
run;
data DistanceC(rename=(gvkeyC=gvkey idC=_id_ outcomeC=outcome outcomeT=outcomeM idT=idM));
set Control(keep=gvkeyC idC pscoreC outcomeC);
do i= 1 to NTreated;
 set Treatment(keep=gvkeyT idT pscoreT outcomeT) point=i nobs=NTreated;
 Distance=abs(pscoreC-pscoreT);
 output;
end;
run;

*Remove obs with missing distance values; 
data distanceT;
	set distanceT;
	Target=1;
	if missing(Distance)=0;
run;

data distanceC;
	set distanceC;
	Target=0;
	if missing(Distance)=0;
run;

/** Closest matches for treated with the Nearest Neighbor Matching Method **/
proc sort data=distanceT;
by gvkey distance;

data ClosestJM1T ClosestJM2T;
set distanceT;
by gvkey distance;
retain cardJmi;  /* cardJmi : number of matches for observation i (the number of elements of the set of matches Jmi) */
if first.gvkey then cardJmi=0;
cardJmi=cardJmi+1;
if cardJmi<=&M then output ClosestJM1T;else output ClosestJM2T;

*Dealing with ties;

data tie(drop=cardJmi);
set ClosestJM1T(keep=gvkey distance cardJmi rename=(distance=distancetie));
where cardJmi=&M;

data ClosestJM2T(drop=distancetie);
merge ClosestJM2T tie;
by gvkey;
if distance=distancetie;

data ClosestJMT;
set ClosestJM1T(drop=cardJmi) ClosestJM2T(drop=cardJmi);

proc summary data=ClosestJMT nway noprint;
class gvkey;
output out=JmT(keep=gvkey _freq_ rename=(_freq_=cardJmi));
run;

proc sort data=ClosestJMT;
by gvkey distance;
run;

data ClosestJMT;
merge JmT ClosestJMT;
by gvkey;
run;

data ClosestJMT;
	set ClosestJMT;
	rename gvkey=gvkeyT;
run;


/** Closest matches for controls with the Nearest Neighbor Matching Method **/
proc sort data=distanceC;
by gvkey distance;

data ClosestJM1C ClosestJM2C;
set distanceC;
by gvkey distance;
retain cardJmi;  /* cardJmi : number of matches for observation i (the number of elements of the set of matches Jmi) */
if first.gvkey then cardJmi=0;
cardJmi=cardJmi+1;
if cardJmi<=&M then output ClosestJM1C;else output ClosestJM2C;

*Dealing with ties;

data tie(drop=cardJmi);
set ClosestJM1C(keep=gvkey distance cardJmi rename=(distance=distancetie));
where cardJmi=&M;

data ClosestJM2C(drop=distancetie);
merge ClosestJM2C tie;
by gvkey;
if distance=distancetie;

data ClosestJMC;
set ClosestJM1C(drop=cardJmi) ClosestJM2C(drop=cardJmi);

proc summary data=ClosestJMC nway noprint;
class gvkey;
output out=JmC(keep=gvkey _freq_ rename=(_freq_=cardJmi));
run;

proc sort data=ClosestJMC;
by gvkey distance;
run;

data ClosestJMC;
merge JmC ClosestJMC;
by gvkey;
run;

data ClosestJMC;
	set ClosestJMC;
	rename gvkey=gvkeyC;
run;

%mend ps_matching;


%let _sdtm=%sysfunc(datetime());

options mcompilenote=ALL;
options SYMBOLGEN MPRINT MLOGIC;

%macro doit1;
%do j=1994 %to 2010;

data sample;
	set comp_extract2;
	if calyear=&j+1;
run;

%ps_matching(data=sample,y=TobinQ1,w=target,x=tobinq_1 tobinq_2 tobinq_3 tobinq_4 tobinq_5 Lnmarketvalue_1 Leverage_1 roa_1 Lnsize_1 capx_1 rd_1 Intangibility_1,M=1,Link=logit,L=2,Lt=1);

data closestjmt;
	set closestjmt;
	calyear=&j+1;
run;

data closestjmc;
	set closestjmc;
	calyear=&j+1;
run;

proc append data=closestjmt base=treated_final force; run;
proc append data=closestjmc base=control_final force; run;

%end;
%mend doit1;
%doit1

%let _edtm=%sysfunc(datetime());
%let _runtm=%sysfunc(putn(&_edtm - &_sdtm, 12.4));
%put It took &_runtm second to run the program;



/* Step 2.6. Include fundamentals data in matched data */

proc sql;
	create table treated_final1 as
	select distinct a.*, b.tobinq_1 as tobinq_1t, b.tobinq_2 as tobinq_2t, b.tobinq_3 as tobinq_3t,
	b.tobinq_4 as tobinq_4t, b.tobinq_5 as tobinq_5t, b.tobinq1 as tobinq1t, b.tobinq2 as tobinq2t, 
	b.tobinq3 as tobinq3t, b.tobinq4 as tobinq4t, b.tobinq5 as tobinq5t, b.Lnmarketvalue_1 as nmarketvalue_1t,
	b.Leverage_1 as Leverage_1t, b.roa_1 as roa_1t, b.Lnsize_1 as Lnsize_1t, b.capx_1 as capx_1t,
	b.rd_1 as rd_1t, b.Intangibility_1 as Intangibility_1t
	from treated_final as a left join comp_extract2 as b
	on a.gvkeyt=b.gvkey and a.calyear=b.calyear;
quit;

proc sql;
	create table treated_final1 as
	select distinct a.*, b.tobinq_1 as tobinq_1c, b.tobinq_2 as tobinq_2c, b.tobinq_3 as tobinq_3c,
	b.tobinq_4 as tobinq_4c, b.tobinq_5 as tobinq_5c, b.tobinq1 as tobinq1c, b.tobinq2 as tobinq2c, 
	b.tobinq3 as tobinq3c, b.tobinq4 as tobinq4c, b.tobinq5 as tobinq5c, b.Lnmarketvalue_1 as nmarketvalue_1c,
	b.Leverage_1 as Leverage_1c, b.roa_1 as roa_1c, b.Lnsize_1 as Lnsize_1c, b.capx_1 as capx_1c,
	b.rd_1 as rd_1c, b.Intangibility_1 as Intangibility_1c
	from treated_final1 as a left join comp_extract2 as b
	on a.gvkeyc=b.gvkey and a.calyear=b.calyear;
quit;


proc sql;
	create table control_final1 as
	select distinct a.*, b.tobinq_1 as tobinq_1t, b.tobinq_2 as tobinq_2t, b.tobinq_3 as tobinq_3t,
	b.tobinq_4 as tobinq_4t, b.tobinq_5 as tobinq_5t, b.tobinq1 as tobinq1t, b.tobinq2 as tobinq2t, 
	b.tobinq3 as tobinq3t, b.tobinq4 as tobinq4t, b.tobinq5 as tobinq5t, b.Lnmarketvalue_1 as nmarketvalue_1t,
	b.Leverage_1 as Leverage_1t, b.roa_1 as roa_1t, b.Lnsize_1 as Lnsize_1t, b.capx_1 as capx_1t,
	b.rd_1 as rd_1t, b.Intangibility_1 as Intangibility_1t
	from control_final as a left join comp_extract2 as b
	on a.gvkeyt=b.gvkey and a.calyear=b.calyear;
quit;

proc sql;
	create table control_final1 as
	select distinct a.*, b.tobinq_1 as tobinq_1c, b.tobinq_2 as tobinq_2c, b.tobinq_3 as tobinq_3c,
	b.tobinq_4 as tobinq_4c, b.tobinq_5 as tobinq_5c, b.tobinq1 as tobinq1c, b.tobinq2 as tobinq2c, 
	b.tobinq3 as tobinq3c, b.tobinq4 as tobinq4c, b.tobinq5 as tobinq5c, b.Lnmarketvalue_1 as nmarketvalue_1c,
	b.Leverage_1 as Leverage_1c, b.roa_1 as roa_1c, b.Lnsize_1 as Lnsize_1c, b.capx_1 as capx_1c,
	b.rd_1 as rd_1c, b.Intangibility_1 as Intangibility_1c
	from control_final1 as a left join comp_extract2 as b
	on a.gvkeyc=b.gvkey and a.calyear=b.calyear;
quit;


/* Step 2.7. Table4A results */

data treated_final1;
	set treated_final1;
	tobinq_1=sum(tobinq_1t,-tobinq_1c);
	tobinq_2=sum(tobinq_2t,-tobinq_2c);
	tobinq_3=sum(tobinq_3t,-tobinq_3c);
	tobinq_4=sum(tobinq_4t,-tobinq_4c);
	tobinq_5=sum(tobinq_5t,-tobinq_5c);
	Lnmarketvalue_1=sum(nmarketvalue_1t,-nmarketvalue_1c);
	Leverage_1=sum(Leverage_1t,-Leverage_1c);
	roa_1=sum(roa_1t,-roa_1c);
	Lnsize_1=sum(Lnsize_1t,-Lnsize_1c);
	capx_1=sum(capx_1t,-capx_1c);
	rd_1=sum(rd_1t,-rd_1c);
	Intangibility_1=sum(Intangibility_1t,-Intangibility_1c);
run;

*means;
proc means data=treated_final1 noprint;
  by target;
  var tobinq_1t tobinq_2t tobinq_3t tobinq_4t tobinq_5t nmarketvalue_1t Leverage_1t roa_1t Lnsize_1t capx_1t rd_1t Intangibility_1t
tobinq_1c tobinq_2c tobinq_3c tobinq_4c tobinq_5c nmarketvalue_1c Leverage_1c roa_1c Lnsize_1c capx_1c rd_1c Intangibility_1c
tobinq_1 tobinq_2 tobinq_3 tobinq_4 tobinq_5 Lnmarketvalue_1 Leverage_1 roa_1 Lnsize_1 capx_1 rd_1 Intangibility_1;
  output out=table4a mean=tobinq_1t tobinq_2t tobinq_3t tobinq_4t tobinq_5t nmarketvalue_1t Leverage_1t roa_1t Lnsize_1t capx_1t rd_1t Intangibility_1t
tobinq_1c tobinq_2c tobinq_3c tobinq_4c tobinq_5c nmarketvalue_1c Leverage_1c roa_1c Lnsize_1c capx_1c rd_1c Intangibility_1c
tobinq_1 tobinq_2 tobinq_3 tobinq_4 tobinq_5 Lnmarketvalue_1 Leverage_1 roa_1 Lnsize_1 capx_1 rd_1 Intangibility_1;
quit;

*p-value;
proc means data=treated_final1 noprint;
  by target;
  var tobinq_1 tobinq_2 tobinq_3 tobinq_4 tobinq_5 Lnmarketvalue_1 Leverage_1 roa_1 Lnsize_1 capx_1 rd_1 Intangibility_1;
  output out=table4b prt=tobinq_1 tobinq_2 tobinq_3 tobinq_4 tobinq_5 Lnmarketvalue_1 Leverage_1 roa_1 Lnsize_1 capx_1 rd_1 Intangibility_1;
quit;

*Export csv;
proc export data=table4a outfile="C:\Users\KUTHURU\Downloads\Laptop\Semester 3\Corporate Finance Slava\Homework-3\Table4a.csv" 
dbms=csv replace;
run;

proc export data=table4b outfile="C:\Users\KUTHURU\Downloads\Laptop\Semester 3\Corporate Finance Slava\Homework-3\Table4b.csv" 
dbms=csv replace;
run;

proc export data=treated_final1 outfile="C:\Users\KUTHURU\Downloads\Laptop\Semester 3\Corporate Finance Slava\Homework-3\fig1.csv" 
dbms=csv replace;
run;


/*********************************************************/
/*Step 3. Extension: improve sales to fend off activists */
/*********************************************************/
*here, sales variables enters covariates in matching;


%let _sdtm=%sysfunc(datetime());

options mcompilenote=ALL;
options SYMBOLGEN MPRINT MLOGIC;

%macro doit1;
%do j=1994 %to 2010;

data sample;
	set comp_extract2;
	if calyear=&j+1;
run;

%ps_matching(data=sample,y=TobinQ1,w=target,x=tobinq_1 tobinq_2 tobinq_3 tobinq_4 tobinq_5 Lnmarketvalue_1 Leverage_1 roa_1 Lnsize_1 Lnsale_1 capx_1 rd_1 Intangibility_1,M=1,Link=logit,L=2,Lt=1);

data closestjmt;
	set closestjmt;
	calyear=&j+1;
run;

data closestjmc;
	set closestjmc;
	calyear=&j+1;
run;

proc append data=closestjmt base=treated_final force; run;
proc append data=closestjmc base=control_final force; run;

%end;
%mend doit1;
%doit1

%let _edtm=%sysfunc(datetime());
%let _runtm=%sysfunc(putn(&_edtm - &_sdtm, 12.4));
%put It took &_runtm second to run the program;



/* Step 3.1. Include fundamentals data in matched data */

proc sql;
	create table treated_final1 as
	select distinct a.*, b.tobinq_1 as tobinq_1t, b.tobinq_2 as tobinq_2t, b.tobinq_3 as tobinq_3t,
	b.tobinq_4 as tobinq_4t, b.tobinq_5 as tobinq_5t, b.tobinq1 as tobinq1t, b.tobinq2 as tobinq2t, 
	b.tobinq3 as tobinq3t, b.tobinq4 as tobinq4t, b.tobinq5 as tobinq5t, b.Lnmarketvalue_1 as Lnmarketvalue_1t,
	b.Leverage_1 as Leverage_1t, b.roa_1 as roa_1t, b.Lnsize_1 as Lnsize_1t, b.capx_1 as capx_1t,
	b.rd_1 as rd_1t, b.Intangibility_1 as Intangibility_1t, b.Lnsale_1 as Lnsale_1t
	from treated_final as a left join comp_extract2 as b
	on a.gvkeyt=b.gvkey and a.calyear=b.calyear;
quit;

proc sql;
	create table treated_final1 as
	select distinct a.*, b.tobinq_1 as tobinq_1c, b.tobinq_2 as tobinq_2c, b.tobinq_3 as tobinq_3c,
	b.tobinq_4 as tobinq_4c, b.tobinq_5 as tobinq_5c, b.tobinq1 as tobinq1c, b.tobinq2 as tobinq2c, 
	b.tobinq3 as tobinq3c, b.tobinq4 as tobinq4c, b.tobinq5 as tobinq5c, b.Lnmarketvalue_1 as Lnmarketvalue_1c,
	b.Leverage_1 as Leverage_1c, b.roa_1 as roa_1c, b.Lnsize_1 as Lnsize_1c, b.capx_1 as capx_1c,
	b.rd_1 as rd_1c, b.Intangibility_1 as Intangibility_1c, b.Lnsale_1 as Lnsale_1c
	from treated_final1 as a left join comp_extract2 as b
	on a.gvkeyc=b.gvkey and a.calyear=b.calyear;
quit;


proc sql;
	create table control_final1 as
	select distinct a.*, b.tobinq_1 as tobinq_1t, b.tobinq_2 as tobinq_2t, b.tobinq_3 as tobinq_3t,
	b.tobinq_4 as tobinq_4t, b.tobinq_5 as tobinq_5t, b.tobinq1 as tobinq1t, b.tobinq2 as tobinq2t, 
	b.tobinq3 as tobinq3t, b.tobinq4 as tobinq4t, b.tobinq5 as tobinq5t, b.Lnmarketvalue_1 as Lnmarketvalue_1t,
	b.Leverage_1 as Leverage_1t, b.roa_1 as roa_1t, b.Lnsize_1 as Lnsize_1t, b.capx_1 as capx_1t,
	b.rd_1 as rd_1t, b.Intangibility_1 as Intangibility_1t, b.Lnsale_1 as Lnsale_1t
	from control_final as a left join comp_extract2 as b
	on a.gvkeyt=b.gvkey and a.calyear=b.calyear;
quit;

proc sql;
	create table control_final1 as
	select distinct a.*, b.tobinq_1 as tobinq_1c, b.tobinq_2 as tobinq_2c, b.tobinq_3 as tobinq_3c,
	b.tobinq_4 as tobinq_4c, b.tobinq_5 as tobinq_5c, b.tobinq1 as tobinq1c, b.tobinq2 as tobinq2c, 
	b.tobinq3 as tobinq3c, b.tobinq4 as tobinq4c, b.tobinq5 as tobinq5c, b.Lnmarketvalue_1 as Lnmarketvalue_1c,
	b.Leverage_1 as Leverage_1c, b.roa_1 as roa_1c, b.Lnsize_1 as Lnsize_1c, b.capx_1 as capx_1c,
	b.rd_1 as rd_1c, b.Intangibility_1 as Intangibility_1c, b.Lnsale_1 as Lnsale_1c
	from control_final1 as a left join comp_extract2 as b
	on a.gvkeyc=b.gvkey and a.calyear=b.calyear;
quit;


/* Step 3.2. Extension table results */

data treated_final1;
	set treated_final1;
	tobinq_1=sum(tobinq_1t,-tobinq_1c);
	tobinq_2=sum(tobinq_2t,-tobinq_2c);
	tobinq_3=sum(tobinq_3t,-tobinq_3c);
	tobinq_4=sum(tobinq_4t,-tobinq_4c);
	tobinq_5=sum(tobinq_5t,-tobinq_5c);
	Lnmarketvalue_1=sum(Lnmarketvalue_1t,-Lnmarketvalue_1c);
	Leverage_1=sum(Leverage_1t,-Leverage_1c);
	roa_1=sum(roa_1t,-roa_1c);
	Lnsize_1=sum(Lnsize_1t,-Lnsize_1c);
	capx_1=sum(capx_1t,-capx_1c);
	rd_1=sum(rd_1t,-rd_1c);
	Intangibility_1=sum(Intangibility_1t,-Intangibility_1c);
	Lnsale_1=sum(Lnsale_1t,-Lnsale_1c);
run;

*means;
proc means data=treated_final1 noprint;
  by target;
  var tobinq_1t tobinq_2t tobinq_3t tobinq_4t tobinq_5t Lnmarketvalue_1t Leverage_1t roa_1t Lnsize_1t capx_1t rd_1t Intangibility_1t Lnsale_1t
tobinq_1c tobinq_2c tobinq_3c tobinq_4c tobinq_5c Lnmarketvalue_1c Leverage_1c roa_1c Lnsize_1c capx_1c rd_1c Intangibility_1c Lnsale_1c
tobinq_1 tobinq_2 tobinq_3 tobinq_4 tobinq_5 Lnmarketvalue_1 Leverage_1 roa_1 Lnsize_1 capx_1 rd_1 Intangibility_1 Lnsale_1;
  output out=table4a mean=tobinq_1t tobinq_2t tobinq_3t tobinq_4t tobinq_5t Lnmarketvalue_1t Leverage_1t roa_1t Lnsize_1t capx_1t rd_1t Intangibility_1t Lnsale_1t
tobinq_1c tobinq_2c tobinq_3c tobinq_4c tobinq_5c Lnmarketvalue_1c Leverage_1c roa_1c Lnsize_1c capx_1c rd_1c Intangibility_1c Lnsale_1c
tobinq_1 tobinq_2 tobinq_3 tobinq_4 tobinq_5 Lnmarketvalue_1 Leverage_1 roa_1 Lnsize_1 capx_1 rd_1 Intangibility_1 Lnsale_1;
quit;

*p-value;
proc means data=treated_final1 noprint;
  by target;
  var tobinq_1 tobinq_2 tobinq_3 tobinq_4 tobinq_5 Lnmarketvalue_1 Leverage_1 roa_1 Lnsize_1 capx_1 rd_1 Intangibility_1 Lnsale_1;
  output out=table4b prt=tobinq_1 tobinq_2 tobinq_3 tobinq_4 tobinq_5 Lnmarketvalue_1 Leverage_1 roa_1 Lnsize_1 capx_1 rd_1 Intangibility_1 Lnsale_1;
quit;

*Export csv;
proc export data=table4a outfile="C:\Users\KUTHURU\Downloads\Laptop\Semester 3\Corporate Finance Slava\Homework-3\Table4a.csv" 
dbms=csv replace;
run;

proc export data=table4b outfile="C:\Users\KUTHURU\Downloads\Laptop\Semester 3\Corporate Finance Slava\Homework-3\Table4b.csv" 
dbms=csv replace;
run;


proc export data=treated_final1 outfile="C:\Users\KUTHURU\Downloads\Laptop\Semester 3\Corporate Finance Slava\Homework-3\fig2.csv" 
dbms=csv replace;
run;
