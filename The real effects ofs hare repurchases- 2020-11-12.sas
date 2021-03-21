proc datasets lib=work kill nolist memtype=data;
quit;

libname CF 'C:\Users\KUTHURU\Downloads\Laptop\Semester 3\Corporate Finance Slava\Special Project'; run;

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
libname crsp '/wrds/crsp/sasdata/a_ccm'; run; *refers to crsp;
libname compa '/wrds/comp/sasdata/naa'; run; *refers to compa;
libname compq '/wrds/comp/sasdata/d_na'; run; *refers to comp quarterly;
libname cmpny '/wrds/comp/sasdata/d_na/company'; run; *refers to company file;
libname pn '/wrds/comp/sasdata/d_na/pension'; run; *compustat pension;
libname ibes '/wrds/ibes/sasdata'; run; *IBES;
libname frb '/wrds/frb/sasdata'; run; *FRB;
libname home '/scratch/bc/Adarsh'; run;
endrsubmit;

/*****************************************************************/
/* 	        Step 1. Obtain Firm-level quarterly data             */
/*****************************************************************/

rsubmit;

options errors=1 noovp;
options nocenter ps=max ls=78;
options mprint source nodate symbolgen macrogen;
options msglevel=i;

*Specify data begin and end dates;
%let begindate = '01Jan1988'd;
%let enddate = '31DEC2001'd; 


/* Step 1.1. Obtain Quarterly Data from Compustat */
data comp_extract;
   format gvkey datadate date_fyend calyear fyearq fyr indfmt consol datafmt popsrc 
          ATQ TSTKQ CSHOPQ CSHIQ PRCRAQ CSHOQ PRCCQ PRSTKCY PSTKQ PSTKRQ SSTKY NIQ CHQ; 
   set compq.fundq
   (where=(missing(ATQ)=0 and ATQ>0 and consol='C' and
           indfmt='INDL' and datafmt='STD' and popsrc='D'));

   *Converts fiscal year into calendar year data as in Daniel and Titman (JF, 2006);
   if (1<=fyr<=5) then date_fyend=intnx('month',mdy(fyr,1,fyearq+1),0,'end');
   else if (6<=fyr<=12) then date_fyend=intnx('month',mdy(fyr,1,fyearq),0,'end');
   calyear=year(date_fyend);
   format date_fyend date9.;

   *Accounting data since calendar year 't-1';
   if year(&begindate)<=year(date_fyend)<=year(&enddate)+1;
   keep gvkey datadate date_fyend calyear fyearq fyr indfmt consol datafmt popsrc 
        ATQ TSTKQ CSHOPQ CSHIQ PRCRAQ CSHOQ PRCCQ PRSTKCY PSTKQ PSTKRQ SSTKY NIQ CHQ;
run;
*897610 obs;


/* Step 1.1.1 Add lag variables */

proc sort data=comp_extract; by gvkey datadate; run;

data comp_extract;
	set comp_extract;
	nrow=_N_;
run;
proc sql;
	create table comp_extract as
	select distinct a.*,b.PSTKQ as PSTKQ_1, b.PSTKRQ as PSTKRQ_1
	from comp_extract as a left join comp_extract as b
	on a.gvkey=b.gvkey and a.nrow=b.nrow+1;
quit;


/* Step 1.2. Obtain SIC Data from Compustat Annual file*/
data comp_extract1;
   format gvkey datadate AT SICH;  
   set compa.funda
   (where=(missing(AT)=0 and AT>0 and consol='C' and
           indfmt='INDL' and datafmt='STD' and popsrc='D'));

   *Exclude regulated utility, transportation and finance firms;
   if SICH>=4700 and SICH<=4829 then delete;
   if SICH>=4910 and SICH<=4949 then delete;
   if SICH>=6000 and SICH<=6999 then delete;

   *Converts fiscal year into calendar year data;
   if (1<=fyr<=5) then date_fyend=intnx('month',mdy(fyr,1,fyear+1),0,'end');
   else if (6<=fyr<=12) then date_fyend=intnx('month',mdy(fyr,1,fyear),0,'end');
   calyear=year(date_fyend);
   format date_fyend date9.;

   *Accounting data since calendar year 't-1';
   if year(&begindate)-1<=year(date_fyend)<=year(&enddate)+1;
   keep gvkey datadate date_fyend calyear fyear fyr indfmt consol datafmt popsrc 
        AT SICH;
run;
*197004 obs;

proc sort data=comp_extract1 nodupkey; by gvkey date_fyend sich; run;
*197004 obs: 0 duplicates;

/* Step 1.3. Merge SIC Data from Compustat Annual file with Quarterly data*/
proc sql;
	create table comp_extract as
	select distinct a.*,b.SICH as SIC
	from comp_extract as a left join comp_extract1 as b
	on a.gvkey=b.gvkey and year(a.date_fyend)=year(b.date_fyend);
quit;
*897610 obs;

proc sort data=comp_extract nodupkey; by gvkey datadate sic; run;
*897610 obs: 0 duplicates;

/* Step 1.4 CRSP-Compustat merge */
proc sort data=crspa.stocknames out=crsp;
	by permno nameenddt;
run;
proc sql;
	create table crsp_m as
	select a.*, b.ticker
	from crspa.msf(keep=permno permco cusip date) as a, crsp as b
	where a.permno = b.permno 
	order by a.permco, a.permno, a.date;
quit;
*9798542 obs;

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
*9798542 obs;

*Remove duplicates by year if any;
data crsp_ccm;
	set crsp_ccm;
	year=year(date);
run;
proc sort data=crsp_ccm nodupkey; by gvkey year; run;
*332275 obs: 0 duplicates;

proc sql;
	create table comp_extract2 as
	select distinct a.*,b.*
	from comp_extract as a, crsp_ccm as b
	where a.gvkey=b.gvkey and year(a.date_fyend)=b.year
	order by a.gvkey,a.datadate;
quit;
*415915 obs;


/* Step 1.5 Merge CRSP-Compustat data with t-bill rates */
proc sql;
	create table comp_extract3 as
	select distinct a.*,b.TB_M3 as rate
	from comp_extract2 as a, frb.rates_monthly as b
	where month(a.datadate)=month(b.date) and year(a.datadate)=year(b.date)
	order by a.gvkey,a.datadate;
quit;

/* Step 1.6 Merge above dataset with SUE values from step-4 file */
proc sql;
  create table comp_extract3 as
  select distinct a.*, b.ticker as ibes_ticker
  from comp_extract3 as a, ibes1.idsum as b
  where 
        a.CUSIP = b.CUSIP
    and a.date > b.SDATES;
quit; 
*223465 obs;


endrsubmit;














/*****************************************************************/
/* 	        Step 2. Obtain Descriptive statistics                */
/*****************************************************************/


proc sort data=comp_extract3; by gvkey datadate; run;

data comp_extract3;
	set comp_extract3;
	nrow=_N_;
run;

/* Step 2.1. Add lag treasury stock variable */
proc sql;
	create table comp_extract4 as
	select distinct a.*,b.TSTKQ as TSTKQ_1
	from comp_extract3 as a left join comp_extract3 as b
	on a.gvkey=b.gvkey and a.nrow=b.nrow+1;
quit;

/* Step 2.2. Estimate Net Repurchases */
data comp_extract5;
	set comp_extract4;
	if missing(TSTKQ)=0 and TSTKQ^=0 then netrepo=sum(TSTKQ,-TSTKQ_1);
    else netrepo=sum(CSHOPQ,-CSHIQ);
	*if missing(netrepo)=0 and netrepo<0 then netrepo=0;
	reposhares=netrepo/PRCCQ;
	*drop netrepo reposhares;
run;
	

/* Step 2.3. Add Net Repurchase Indicator */
data comp_extract5;
	set comp_extract5;
	if netrepo>0 then ind=1;
	else ind=0;
	repout=reposhares/cshoq;
	repoassets=netrepo/atq;
run;

/* Step 2.4. Winsorize the variables*/
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
%winsor(dsetin=comp_extract5, dsetout=comp_extract5, byvar=none, 
vars=netrepo repout repoassets, type=winsor, pctl=1 99);


/* Step 2.5. Summary Stats for full sample*/
PROC UNIVARIATE DATA=comp_extract5 noprint;
  VAR ind;
  output out = temp
	pctlpts = (1,5,25,50,75,95,99) pctlpre = p_
	mean=mean std=stddev n=n;
RUN; 
data temp; format stat; set temp; stat='Postitve net repo'; run;


data comp_extract6;
	set comp_extract5;
	if netrepo>0;
run;

PROC UNIVARIATE DATA=comp_extract6 noprint;
  VAR netrepo;
  output out = temp1
	pctlpts = (1,5,25,50,75,95,99) pctlpre = p_
	mean=mean std=stddev n=n;
RUN; 
data temp1; format stat; set temp1; stat='Net repo'; run;

PROC UNIVARIATE DATA=comp_extract6 noprint;
  VAR repout;
  output out = temp2
	pctlpts = (1,5,25,50,75,95,99) pctlpre = p_
	mean=mean std=stddev n=n;
RUN; 
data temp2; format stat; set temp2; stat='Rep Out'; run;

PROC UNIVARIATE DATA=comp_extract6 noprint;
  VAR repoassets;
  output out = temp3
	pctlpts = (1,5,25,50,75,95,99) pctlpre = p_
	mean=mean std=stddev n=n;
RUN; 
data temp3; format stat; set temp3; stat='Repo Assets'; run;

proc append data=temp base=result; run;
proc append data=temp1 base=result force; run;
proc append data=temp2 base=result force; run;
proc append data=temp3 base=result force; run;

proc download data=comp_extract5 out=comp_extract5; run;
proc download data=result out=result; run;
endrsubmit;

*Export csv;
proc export data=result outfile="C:\Users\KUTHURU\Downloads\Laptop\Semester 3\Corporate Finance Slava\Special Project\Table1a.csv" 
dbms=csv replace;
run;

proc export data=comp_extract5 outfile="C:\Users\KUTHURU\Desktop\Data.csv" 
dbms=csv replace;
run;
