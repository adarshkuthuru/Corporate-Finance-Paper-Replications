proc datasets lib=work kill nolist memtype=data;
quit;

libname CF 'C:\Users\KUTHURU\Downloads\Laptop\Semester 3\Corporate Finance Slava\Homework-6'; run;

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
libname home '/home/bc/adarshkp/corp fin'; run;
libname temp '/scratch/bc/Adarsh'; run;
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
          ATQ TSTKQ CSHOPQ CSHIQ PRCRAQ CSHOQ PRCCQ PRSTKCY PSTKQ PSTKRQ SSTKY NIQ CHEQ EPSPXQ SALEQ; 
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
        ATQ TSTKQ CSHOPQ CSHIQ PRCRAQ CSHOQ PRCCQ PRSTKCY PSTKQ PSTKRQ SSTKY NIQ CHEQ EPSPXQ SALEQ;
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
	select distinct a.*,b.PSTKQ as PSTKQ_1, b.PSTKRQ as PSTKRQ_1, b.CSHOQ as CSHOQ_1, b.EPSPXQ as EPSPXQ_1, b.SALEQ AS SALEQ_1
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
from crsp_m as a, crsp.ccmxpf_lnkhist as b
where a.permno=b.lpermno and a.permco = b.lpermco
	and (b.LINKDT <= intnx('month',a.date,0,'E') ) 
	and (intnx('month',a.date,0,'B') <= b.LINKENDDT or missing(b.LINKENDDT) )
	and b.lpermno ^=. and b.LINKTYPE in ("LU","LC")
order by a.permno, a.date;
quit;
*9798542 obs;

*Remove duplicates by year;
data crsp_ccm;
	set crsp_ccm;
	year=year(date);
run;
proc sort data=crsp_ccm nodupkey; by gvkey year; run;
*332275 obs;

proc sql;
	create table comp_extract2 as
	select distinct a.*,b.*
	from comp_extract as a, crsp_ccm as b
	where a.gvkey=b.gvkey and year(a.date_fyend)=b.year
	order by a.gvkey,a.datadate;
quit;
*640955 obs;

/* Step 1.5 Merge CRSP-Compustat data with t-bill rates */
proc sql;
	create table comp_extract3 as
	select distinct a.*,b.TB_M3 as rate
	from comp_extract2 as a, frb.rates_monthly as b
	where month(a.datadate)=month(b.date) and year(a.datadate)=year(b.date)
	order by a.gvkey,a.datadate;
quit;

/* Step 1.6 Merge above dataset with SUE from step-4 file*/
proc sql;
  create table comp_extract3 as
  select distinct a.*, b.ue1 as ue, b.sue1 as sue
  from comp_extract3 as a, home.comp_final as b
  where a.gvkey=b.gvkey and a.datadate=b.datadate;
quit; 
*334775 obs;


/*****************************************************************/
/* 	        Step 2. Obtain Descriptive statistics                */
/*****************************************************************/
proc sql;
	drop table result;
run;

/* Step 2.1. Estimate Net Repurchases and #Repurchased shares*/
data comp_extract4;
	set comp_extract3;
	repo=SUM(PRSTKCY,PSTKQ,-PSTKQ_1);
	reposhares=SUM(CSHOQ_1,SSTKY,PSTKQ,-PSTKQ_1,-CSHOQ_1);
	repoperc=(reposhares/CSHOQ_1)*100;
	saleqg=(sum(saleq,-saleq_1)/saleq_1)*100;
	if PRSTKCY>0.01;
	if repoperc>20 then delete;
run;
*71137 obs;


/* Step 2.2. Winsorize the variables*/
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
%winsor(dsetin=comp_extract4, dsetout=comp_extract4, byvar=none, 
vars=repo reposhares repoperc niq saleq saleqg prccq atq cheq cheq_1 epspxq epspxq_1 cshoq cshoq_1, type=winsor, pctl=1 99);

proc download data=comp_extract4 out=comp_extract4; run;


/* Step 2.5. Summary Stats for full sample*/
PROC UNIVARIATE DATA=comp_extract4 noprint;
  VAR repo;
  output out = temp
	pctlpts = (1,5,25,50,75,95,99) pctlpre = p_
	mean=mean std=stddev n=n;
RUN; 
data temp; format stat; set temp; stat='Repo'; format stat $12; run;



PROC UNIVARIATE DATA=comp_extract4 noprint;
  VAR reposhares;
  output out = temp1
	pctlpts = (1,5,25,50,75,95,99) pctlpre = p_
	mean=mean std=stddev n=n;
RUN; 
data temp1; format stat; set temp1; stat='Repo shares'; run;

PROC UNIVARIATE DATA=comp_extract4 noprint;
  VAR repoperc;
  output out = temp2
	pctlpts = (1,5,25,50,75,95,99) pctlpre = p_
	mean=mean std=stddev n=n;
RUN; 
data temp2; format stat; set temp2; stat='Repo perc'; run;

PROC UNIVARIATE DATA=comp_extract4 noprint;
  VAR saleq;
  output out = temp3
	pctlpts = (1,5,25,50,75,95,99) pctlpre = p_
	mean=mean std=stddev n=n;
RUN; 
data temp3; format stat; set temp3; stat='Sales'; run;

PROC UNIVARIATE DATA=comp_extract4 noprint;
  VAR saleqg;
  output out = temp4
	pctlpts = (1,5,25,50,75,95,99) pctlpre = p_
	mean=mean std=stddev n=n;
RUN; 
data temp4; format stat; set temp4; stat='Sales growth'; run;

PROC UNIVARIATE DATA=comp_extract4 noprint;
  VAR prccq;
  output out = temp5
	pctlpts = (1,5,25,50,75,95,99) pctlpre = p_
	mean=mean std=stddev n=n;
RUN; 
data temp5; format stat; set temp5; stat='Price'; run;

PROC UNIVARIATE DATA=comp_extract4 noprint;
  VAR atq;
  output out = temp6
	pctlpts = (1,5,25,50,75,95,99) pctlpre = p_
	mean=mean std=stddev n=n;
RUN; 
data temp6; format stat; set temp6; stat='Assets'; run;

PROC UNIVARIATE DATA=comp_extract4 noprint;
  VAR cheq;
  output out = temp7
	pctlpts = (1,5,25,50,75,95,99) pctlpre = p_
	mean=mean std=stddev n=n;
RUN; 
data temp7; format stat; set temp7; stat='Cash'; run;

PROC UNIVARIATE DATA=comp_extract4 noprint;
  VAR epspxq;
  output out = temp8
	pctlpts = (1,5,25,50,75,95,99) pctlpre = p_
	mean=mean std=stddev n=n;
RUN; 
data temp8; format stat; set temp8; stat='EPS'; run;

proc append data=temp base=result; run;
proc append data=temp1 base=result force; run;
proc append data=temp2 base=result force; run;
proc append data=temp3 base=result force; run;
proc append data=temp4 base=result force; run;
proc append data=temp5 base=result force; run;
proc append data=temp6 base=result force; run;
proc append data=temp7 base=result force; run;
proc append data=temp8 base=result force; run;

proc download data=result out=result; run;
endrsubmit;

*Round up decimal values to 2 in SAS dataset;
data result;
set result;
array _nums {*} _numeric_;
do i = 1 to dim(_nums);
  _nums{i} = round(_nums{i},.01);
end;
drop i;
run;

*Export to LaTeX table;
ods tagsets.tablesonlylatex file="C:\Users\KUTHURU\Downloads\Laptop\Semester 3\Corporate Finance Slava\Homework-6\table1.tex";
proc print data=result;
run;
ods tagsets.tablesonlylatex close;




/*****************************************************************/
/* 	                     Step 3. Figure-2                        */
/*****************************************************************/

/* Step 3.1. Estimate Pre-buyback and post-buyback EPS and AsifEPS1, AsifEPS2 */
data comp_extract4;
	set comp_extract4;
	qtrdate=intnx('qtr',datadate,0,'E'); *end of quarter date;
	preEPS=epspxq_1/cshoq_1;
	rate1=rate/100;
	w=intck('day',Datadate,qtrdate)/90;
	postEPS=(epspxq_1-w*(cshoq-cshoq_1)*prccq*rate1)/(cshoq_1-w*(cshoq-cshoq_1));
	AsifEPS1=niq/(cshoq_1+0.5*(sstky+pstkq-pstkq_1));
	AsifEPS2=(niq-w*(cshoq-cshoq_1)*prccq*rate1)/(cshoq_1+0.5*(sstky+pstkq-pstkq_1));
	if (preEPS/prccq)>rate1 then acc1=1;
	else acc1=0;
	if (postEPS/prccq)>rate1 then acc2=1;
	else acc2=0;
	preerror=(preEPS-AsifEPS1)/AsifEPS1;
run;



/* 
Step 3.2. Count number of firm-quarters which are deemed accretive 

proc sql;
	create table sample as
	select distinct sum(acc1) as acc1, sum(acc2) as acc2
	from comp_extract4;
quit;
*/

*******************************************************
					Density Histogram
******************************************************;

data comp_extract5;
	set comp_extract4;
	if acc1=1 or acc2=1;
run;

data class;
set comp_extract5;
*w1 = round(factor1 - 0.025,0.05 ) + 0.025;
w1 = round(preerror, 0.1);
label w1 = 'pre-repurchase forecast error';
run;
proc freq data=class;
tables w1 / out=freqs1 noprint;
run;
proc template;
define statgraph classhist;
mvar x;
begingraph;
entrytitle 'Pre-repurchase forecast error Vs Percent of Firms';
layout overlay;
histogramparm x=x y=count;
endlayout;
endgraph;
end;
run;
%let x = w1;
proc sgrender data=freqs1 template=classhist;
run;



*******************************************************
					Density Histogram-2
******************************************************;

title 'Pre-repurchase forecast error Vs Percent of Firms';
ods graphics on;
proc univariate data=comp_extract5 noprint;
   histogram preerror1 / midpoints    = -0.2 to 0.2 by .01
                     rtinclude
                     outhistogram = OutMdpts
                     odstitle     = title;
run;

proc print data=OutMdpts;
run;
