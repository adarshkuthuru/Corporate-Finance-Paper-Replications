proc datasets lib=work kill nolist memtype=data;
quit;

libname CF 'C:\Users\KUTHURU\Downloads\Laptop\Semester 3\Corporate Finance Slava\Homework-4'; run;



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
/* 	Step 1. Obtain value and momentum factors for stocks         */
/*****************************************************************/

rsubmit;

proc sql;
drop table result;
quit;

*Specify CRSP begin and end dates;
%let crspbegdate = '01Jan1976'd;
%let crspenddate = '31DEC2002'd; 

/* Step 1.1. Obtain Book-Equity from Compustat */
data comp_extract;
   format gvkey datadate date_fyend calyear fyear fyr indfmt consol datafmt popsrc 
          TXDITC BE0 BE1 BE_FF EPSPX EBITDA SALE DVC CSHO ADJEX_C;  /*http://www.crsp.com/products/documentation/annual-data-industrial */
   set compa.funda
   (where=(fyr>0 and at>0 and consol='C' and
           indfmt='INDL' and datafmt='STD' and popsrc='D'));

   **BE as in Daniel and Titman (JF, 2006); 
   *Shareholder equity(she);
   if missing(SEQ)=0 then she=SEQ; 
   *SEQ=Stockholders Equity (Total);
   else if missing(CEQ)=0 and missing(PSTK)=0 then she=CEQ+PSTK; 
        *CEQ=Common/Ordinary Equity (Total), PSTK=Preferred/Preference Stock (Capital) (Total);
        else if missing(AT)=0 and missing(LT)=0 then she=AT-LT;
		     *AT=Assets (Total), LT=Liabilities (Total), MIB=Minority Interest (Balance Sheet); 
             else she=.;

   if missing(PSTKRV)=0 then BE0=she-PSTKRV; *PSTKRV=Preferred Stock (Redemption Value);
   else if missing(PSTKL)=0 then BE0=she-PSTKL; *PSTKL=Preferred Stock (Liquidating Value);
        else if missing(PSTK)=0 then BE0=she-PSTK; *PSTK=Preferred/Preference Stock (Capital) (Total);
             else BE0=.;
  

   **BE as in Kayhan and Titman (2003);
   *Book_Equity = Total Assets - [Total Liabilities + Preferred Stock] + Peferred Taxes + Conv. Debt;  
   if AT>0 and LT>0 then BE1=AT-(LT+PSTKL)+TXDITC+DCVT;
   else BE1=.;


   **BE as in Fama and French (JF, 2008); 
   if missing(AT)=0 and missing(LT)=0 and missing(TXDITC)=0 then BE_FF_Temp = (AT-LT) + TXDITC; *AT=Assets (Total), LT=Liabilities (Total);
   else if missing(AT)=0 and missing(LT)=0 and missing(TXDITC)=1 then BE_FF_Temp = (AT-LT);  *TXDITC=Deferred Taxes and Investment Tax Credit;
        else BE_FF_Temp = .;
   if missing(PSTKL)=0 then BE_FF = BE_FF_Temp - PSTKL; *PSTKL=Preferred Stock (Liquidating Value);
   else if missing(PSTKRV)=0 then BE_FF = BE_FF_Temp - PSTKRV; *PSTKRV=Preferred Stock (Redemption Value);
        else if missing(PSTK)=0 then BE_FF = BE_FF_Temp - PSTK; *PSTK=Preferred/Preference Stock (Capital) (Total);
		     else BE_FF = .;
   drop BE_FF_Temp;

   *Converts fiscal year into calendar year data;
   if (1<=fyr<=5) then date_fyend=intnx('month',mdy(fyr,1,fyear+1),0,'end');
   else if (6<=fyr<=12) then date_fyend=intnx('month',mdy(fyr,1,fyear),0,'end');
   calyear=year(date_fyend);
   format date_fyend date9.;

   *Accounting data since calendar year 't-1';
   if year(&crspbegdate)-1<=year(date_fyend)<=year(&crspenddate)+1;
   keep gvkey datadate date_fyend calyear fyear fyr indfmt consol datafmt popsrc 
        TXDITC BE0 BE1 BE_FF EPSPX EBITDA SALE DVC CSHO ADJEX_C;
   *Note that datadate is same as date_fyend;
run;


/* Step 1.2 Calculate BE as in Daniel and Titman (JF, 2006) */
proc sql; /*http://www.listendata.com/2014/04/proc-sql-select-statement.html */
  create table comp_extract_be as 
  select distinct a.gvkey, a.datadate format=date9., a.fyr, a.fyear, a.date_fyend, a.calyear, 
         case 
            when missing(TXDITC)=0 and missing(PRBA)=0 then BE0+TXDITC-PRBA 
            else BE0
         end as BE, BE1, BE_FF
  from comp_extract as a left join pn.aco_pnfnda (keep=gvkey indfmt consol datafmt popsrc datadate prba) as b
  on a.gvkey=b.gvkey and a.indfmt=b.indfmt and a.consol=b.consol and a.datafmt=b.datafmt and 
     a.popsrc=b.popsrc and a.datadate=b.datadate;
quit;


/* Step 1.3 Merge compustat data with linktable */
proc sql;
create table comp_ccm as
select a.*, b.lpermno as permno, b.lpermco as permco, b.linkprim, b.linktype
from compa.funda as a, a_ccm.ccmxpf_lnkhist as b
where a.gvkey=b.gvkey
	and (b.LINKDT <= intnx('month',a.datadate,0,'E') ) 
	and (intnx('month',a.datadate,0,'B') <= b.LINKENDDT or missing(b.LINKENDDT) )
	and b.lpermno ^=. and b.LINKTYPE in ("LU","LC")
order by a.gvkey, a.datadate
;
proc sort data=comp_ccm nodupkey; by gvkey; run;
proc sql;
  create table BE as
  select distinct a.*, b.permno
  from comp_extract_be as a, comp_ccm as b
  where a.gvkey=b.gvkey;
quit;

*Date on which BE is available to the investors;
data BE;
  format gvkey permno datadate investdate BE BE_FF;
  set BE;
  investdate=intnx("month",datadate,6,"E");
  format investdate date9.;
  if BE_FF<0 then delete; *As in FF2008;
  keep gvkey permno datadate investdate BE BE_FF;
run;


/************************************/
/* Step 2. Estimate momentum factor */
/************************************/

/* Step 2.1 Run web query and download the data in Test */
/* Step 2.2 Keep only common stocks and obtan Size */
proc sql;
	create table msf as
	select distinct a.*,b.shrcd
	from crspa.msf as a left join crspa.stocknames as b
	on a.permno=b.permno
	having b.shrcd in (10,11);*SELECT ONLY COMMON STOCKS;
quit;

data MonthlyStockData;
  set msf; 
   Date=intnx("month",date,0,"E");
   format Date date9.;
   if missing(PRC)=0 and missing(SHROUT)=0 and SHROUT>0 then ME=(abs(PRC)*SHROUT*1000)/1000000; *SHROUT SHOULD BE GREATER THAN 0 TO MAKE SENSE;   
   label ME='Market Cap ($M)';
   if missing(ME)=1 then delete;
   keep permno Date PRC RET ME;
run;
proc sort data=MonthlyStockData nodupkey; by permno date; run;
data MonthlyStockData;
  set MonthlyStockData;
  by permno date;
  LagME=lag(ME);
  if first.permno=1 then LagME=.;
run;


***MOM;
data MonthlyStockData;
  format permno past1yrdate Date;
  set MonthlyStockData;
  past1yrdate=intnx("month",Date,-12,"E"); *t-12 month end date at the start of date;
  format past1yrdate date9.;
run;
*Valid past 12-month return data;
proc sql;
  create table MonthlyStockData as
  select distinct a.*
  from MonthlyStockData as a, MonthlyStockData as b
  where a.permno=b.permno and a.past1yrDate<b.Date<=a.Date and missing(b.RET)=0
  group by a.permno, a.Date
  having count(distinct b.Date)=12; *Require good return for all 12 months;
quit;
proc sql;
  create table MonthlyStockData1 as
  select distinct a.*, exp(sum(log(1+b.RET))) - 1 as MOM
  from MonthlyStockData as a, MonthlyStockData as b
  where a.permno=b.permno and a.past1yrDate<b.Date<=intnx("month",a.Date,-1,"E") and missing(b.RET)=0
  group by a.permno, a.Date
  having count(distinct b.Date)=11; *Require good return for all 11 months;
quit;

***Value: Valid BE available to the investor;
proc sql;
  create table MonthlyStockData2 as
  select distinct a.*, b.datadate, b.investDate, b.BE_FF as BE, intck('month',b.investDate,a.Date) as diffmonth
  from MonthlyStockData1 as a, BE as b
  where a.permno=b.permno and b.investDate<=a.Date
  group by a.permno, a.Date
  having a.Date-b.investDate=min(a.Date-b.investDate);
quit;
data MonthlyStockData2;
  format permno past1yrDate Date PRC RET BE ME LagME Value MOM;
  set MonthlyStockData2;
  if diffmonth<=11;
  drop investDate diffmonth past1yrdate;
  Value = BE/ME;
run;



/***************************************/
/* Step 3. Extract data for replication*/
/***************************************/


/* Step 3.1. Obtain Data from Compustat */
data comp_extract;
   format gvkey datadate fyear fyr indfmt consol datafmt popsrc sich dlc dltt at prcc_f csho
	ceq capx ebitda ppent xsga ch;  /*http://www.crsp.com/products/documentation/annual-data-industrial */
   set compa.funda
   (where=(fyr>0 and at>0 and consol='C' and
           indfmt='INDL' and datafmt='STD' and popsrc='D'));

   if missing(at)=0;
   if sich >5999 and sich <7000 then delete; *exclude financial firms;
   if year(&crspbegdate)<=year(datadate)<=year(&crspenddate);
   format datadate date9.;
   keep gvkey datadate fyear fyr indfmt consol datafmt popsrc sich dlc dltt at prcc_f csho
	ceq capx ebitda ppent xsga ch;
run;
*214058 obs; 

*estimate age variable and add to above dataset;
proc sql;
	create table age as
	select distinct gvkey, datadate, min(datadate) as start_date
	from compa.funda
	group by gvkey;
quit;

proc sql;
	create table comp_extract as
	select distinct a.*, intck('year',b.start_date,b.datadate) as age
	from comp_extract as a left join age as b
	on a.gvkey=b.gvkey and a.datadate=b.datadate;
quit;


*merge with company dataset for ipodate and incorp data;
proc sql;
	create table comp_extract as
	select distinct a.*, b.ipodate, b.incorp, b.state
	from comp_extract as a left join cmpny.company as b
	on a.gvkey=b.gvkey;
quit;

data comp_extract;
   format gvkey datadate fyear fyr indfmt consol datafmt popsrc sich dlc dltt at prcc_f csho
	ceq capx ebitda ppent incorp ipodate state;  /*http://www.crsp.com/products/documentation/annual-data-industrial */
   set comp_extract;
   if missing(ipodate)=0; 
   if missing(incorp)=0;
run;
*51737 obs;

*add lag at and ppent variables;
proc sql;
	create table comp_extract as
	select distinct a.*, b.at as at_1, b.ppent as ppent_1
	from comp_extract as a left join compa.funda as b
	on a.gvkey=b.gvkey and year(a.datadate)=year(b.datadate)+1;
quit;

proc sort data=comp_extract nodupkey; by gvkey datadate; run;

/* Step 3.2. Estimate variables */
data comp_extract;
	set comp_extract;
	size=log(at);*at in millions;
	ROA=ebitda/at; *ebidta, at in millions;
	CAPX=capx/at;
	PPEG=(sum((ppent/at),-(ppent_1/at_1))/(ppent_1/at_1)); *ppen, at in millions;
	AG=(sum(at,-at_1)/at_1); *Asset growth, at in millions; 
	CH=ch/at; *cash in miilions;
	SGA=xsga/at; *xsga in millions;
	Leverage=sum(dltt,dlc)/at; *dltt, dlc, at in millions;
	sizesq=size*size;
	agesq=age*age;
	calyear=year(datadate);
run; 
*51737 obs;

**************************************************************************
/* Step 3.2.1 Filling missing sich values;
proc sql;
	create table sic as
	select distinct gvkey, max(sich) as sic
	from comp_extract
	group by gvkey;
quit;

*add sic variable to main dataset;
proc sql;
	create table comp_extract as
	select distinct a.*, b.sic
	from comp_extract as a left join sic as b
	on a.gvkey=b.gvkey;
quit;

/* Step 3.2.2. Fama-French Industry clssification*/

%macro ff38(dsin=, dsout=, sicvar=sic, varname=ff38);

	data &dsout;
	set &dsin;
	
	/* industry assignments */
	if ( &sicvar ge 0100 and &sicvar le 0999) then &varname= 1;
	if ( &sicvar ge 1000 and &sicvar le 1299) then &varname= 2;
	if ( &sicvar ge 1300 and &sicvar le 1399) then &varname= 3;
	if ( &sicvar ge 1400 and &sicvar le 1499) then &varname= 4;
	if ( &sicvar ge 1500 and &sicvar le 1799) then &varname= 5;
	if ( &sicvar ge 2000 and &sicvar le 2099) then &varname= 6;
	if ( &sicvar ge 2100 and &sicvar le 2199) then &varname= 7;
	if ( &sicvar ge 2200 and &sicvar le 2299) then &varname= 8;
	if ( &sicvar ge 2300 and &sicvar le 2399) then &varname= 9;
	if ( &sicvar ge 2400 and &sicvar le 2499) then &varname=10;
	if ( &sicvar ge 2500 and &sicvar le 2599) then &varname=11;
	if ( &sicvar ge 2600 and &sicvar le 2661) then &varname=12;
	if ( &sicvar ge 2700 and &sicvar le 2799) then &varname=13;
	if ( &sicvar ge 2800 and &sicvar le 2899) then &varname=14;
	if ( &sicvar ge 2900 and &sicvar le 2999) then &varname=15;
	if ( &sicvar ge 3000 and &sicvar le 3099) then &varname=16;
	if ( &sicvar ge 3100 and &sicvar le 3199) then &varname=17;
	if ( &sicvar ge 3200 and &sicvar le 3299) then &varname=18;
	if ( &sicvar ge 3300 and &sicvar le 3399) then &varname=19;
	if ( &sicvar ge 3400 and &sicvar le 3499) then &varname=20;
	if ( &sicvar ge 3500 and &sicvar le 3599) then &varname=21;
	if ( &sicvar ge 3600 and &sicvar le 3699) then &varname=22;
	if ( &sicvar ge 3700 and &sicvar le 3799) then &varname=23;
	if ( &sicvar ge 3800 and &sicvar le 3879) then &varname=24;
	if ( &sicvar ge 3900 and &sicvar le 3999) then &varname=25;
	if ( &sicvar ge 4000 and &sicvar le 4799) then &varname=26;
	if ( &sicvar ge 4800 and &sicvar le 4829) then &varname=27;
	if ( &sicvar ge 4830 and &sicvar le 4899) then &varname=28;
	if ( &sicvar ge 4900 and &sicvar le 4949) then &varname=29;
	if ( &sicvar ge 4950 and &sicvar le 4959) then &varname=30;
	if ( &sicvar ge 4960 and &sicvar le 4969) then &varname=31;
	if ( &sicvar ge 4970 and &sicvar le 4979) then &varname=32;
	if ( &sicvar ge 5000 and &sicvar le 5199) then &varname=33;
	if ( &sicvar ge 5200 and &sicvar le 5999) then &varname=34;
	if ( &sicvar ge 6000 and &sicvar le 6999) then &varname=35;
	if ( &sicvar ge 7000 and &sicvar le 8999) then &varname=36;
	if ( &sicvar ge 9000 and &sicvar le 9999) then &varname=37;

	/*  Fama french siccodes file does not include industry codes for 'other'
		Set it to 38 (i.e. 'other') if not yet set
	 */
	if &varname eq . then &varname = 38;
	run;

%mend;

/* invoke macro to classify industry */
%ff38(dsin=comp_extract, dsout=comp_extract, varname=ff38);

/* Step 3.3. Winsorize the variables*/
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
vars=size sizesq ROA CAPX PPEG AG CH SGA Leverage, type=winsor, pctl=1 99);


/* Step 3.4. Desciptive stats */
PROC UNIVARIATE DATA=comp_extract noprint;
  VAR ROA;
  output out = temp1
	pctlpts = 25 to 75 by 50 pctlpre = p_
	mean=mean std=stddev n=n;
RUN; 
data temp1; format stat; set temp1; stat='ROA'; run;

PROC UNIVARIATE DATA=comp_extract noprint;
  VAR CAPX;
  output out = temp2
	pctlpts = 25 to 75 by 50 pctlpre = p_
	mean=mean std=stddev n=n;
RUN; 
data temp2; format stat; set temp2; stat='CAPX'; run;

PROC UNIVARIATE DATA=comp_extract noprint;
  VAR PPEG;
  output out = temp3
	pctlpts = 25 to 75 by 50 pctlpre = p_
	mean=mean std=stddev n=n;
RUN; 
data temp3; format stat; set temp3; stat='PPEG'; run;

PROC UNIVARIATE DATA=comp_extract noprint;
  VAR AG;
  output out = temp4
	pctlpts = 25 to 75 by 50 pctlpre = p_
	mean=mean std=stddev n=n;
RUN; 
data temp4; format stat; set temp4; stat='AG'; run;

PROC UNIVARIATE DATA=comp_extract noprint;
  VAR CH;
  output out = temp5
	pctlpts = 25 to 75 by 50 pctlpre = p_
	mean=mean std=stddev n=n;
RUN; 
data temp5; format stat; set temp5; stat='CH'; run;

PROC UNIVARIATE DATA=comp_extract noprint;
  VAR SGA;
  output out = temp6
	pctlpts = 25 to 75 by 50 pctlpre = p_
	mean=mean std=stddev n=n;
RUN; 
data temp6; format stat; set temp6; stat='SGA'; run;

PROC UNIVARIATE DATA=comp_extract noprint;
  VAR Leverage;
  output out = temp7
	pctlpts = 25 to 75 by 50 pctlpre = p_
	mean=mean std=stddev n=n;
RUN; 
data temp7; format stat; set temp7; stat='Leverage'; run;


proc append data=temp1 base=result force; run;
proc append data=temp2 base=result force; run;
proc append data=temp3 base=result force; run;
proc append data=temp4 base=result force; run;
proc append data=temp5 base=result force; run;
proc append data=temp6 base=result force; run;
proc append data=temp7 base=result force; run;

*drop tables;
proc sql;
drop table temp1, temp2, temp3, temp4, temp5, temp6, temp7;
quit;


/* Step 3.4.1. Merge comp_extract with value and momentum factors */

/* Step 3.4.1.1 Merge compustat data with linktable */
proc sql;
create table comp_ccm as
select a.*, b.lpermno as permno, b.lpermco as permco, b.linkprim, b.linktype
from compa.funda as a, a_ccm.ccmxpf_lnkhist as b
where a.gvkey=b.gvkey
	and (b.LINKDT <= intnx('month',a.datadate,0,'E') ) 
	and (intnx('month',a.datadate,0,'B') <= b.LINKENDDT or missing(b.LINKENDDT) )
	and b.lpermno ^=. and b.LINKTYPE in ("LU","LC")
order by a.gvkey, a.datadate
;
proc sort data=comp_ccm nodupkey; by gvkey; run;
proc sql;
  create table comp_extract as
  select distinct a.*, b.permno
  from comp_extract as a, comp_ccm as b
  where a.gvkey=b.gvkey;
quit;
proc sql;
  create table comp_extract1 as
  select distinct a.*, b.value, b.mom
  from comp_extract as a, MonthlyStockData2 as b
  where a.permno=b.permno and month(a.datadate)=month(b.date) and year(a.datadate)=year(b.date)
  group by a.permno, a.datadate;
quit;


proc download data=result out=result; run;
proc download data=comp_extract1 out=comp_extract; run;
endrsubmit;


*Export csv;
proc export data=result outfile="C:\Users\KUTHURU\Downloads\Laptop\Semester 3\Corporate Finance Slava\Homework-4\Table1.csv" 
dbms=csv replace;
run;


/* Step 3.5. Import BC law passed states dataset and append it to fundamentals from Compustat*/

***Import BC law date csv;
      data WORK.BC    ;
      %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
      infile 'C:\Users\KUTHURU\Downloads\Laptop\Semester 3\Corporate Finance
 Slava\Homework-4\BCdata.csv' delimiter = ',' MISSOVER DSD lrecl=32767 firstobs=2 ;
         informat State $15. ;
         informat Date mmddyy10. ;
         format State $15. ;
         format Date mmddyy10. ;
      input
                  State  $
                  Date
      ;
      if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
      run;

*33 obs;

*convert numeric date format;
data BC;
	set BC;
	year=year(date);
	format Date date9.;
run;

***Import incorp codes csv and merge them;
PROC IMPORT OUT= Incorp
            DATAFILE= "C:\Users\KUTHURU\Downloads\Laptop\Semester 3\Corporate Finance Slava\Homework-4\Incorp codes.csv" 
            DBMS=CSV REPLACE;
RUN;

*Merge;
proc sql;
	create table BC as
	select distinct a.*,b.code
	from BC as a left join Incorp as b
	on a.State=b.description;
quit;



/* Step 3.6. Include BC dummy which equals 1 after state passed BC law*/
proc sql;
	create table comp_extract1 as
	select distinct a.*, b.date as BC_date
	from comp_extract as a left join bc as b
	on a.incorp=b.code; 
run;

data comp_extract1;
	set comp_extract1;
	if missing(BC_date)=0 and year(datadate)>=year(BC_date) then BC=1;
	else BC=0;
run;


*Export csv;
proc export data=comp_extract1 outfile="C:\Users\KUTHURU\Downloads\Laptop\Semester 3\Corporate Finance Slava\Homework-4\data.csv" 
dbms=csv replace;
run;

*number of firms by state;
proc sql;
	create table firms as
	select distinct incorp, calyear, count(distinct gvkey) as nfirms
	from comp_extract1
	group by incorp, calyear;
quit;


/* Step 3.7. match sample */
proc sort data=bc; by date; run;
proc sort data=firms; by calyear incorp; run;

data bc;
	set bc;
	year_1=year-1;
	if code='TX' then delete;
	n=_N_;
run;

*number of firms in the state, the year before;
proc sql;
	create table bc as
	select distinct a.*, b.nfirms
	from bc as a left join firms as b
	on a.code=b.incorp and a.year_1=b.calyear
	order by date, code;
quit;

data states;
	input incorp $;
	datalines;
	CA
	TX
	CA
	TX
	TX
	CA
	TX
	CA
	CA
	CA
	TX
	TX
	TX
	CA
	CA
	CA
	TX
	TX
	CA
	CA
	TX
	CA
	TX
	CA
	CA
	CA
	TX
	CA
	CA
	TX
	CA
	CA
;

data bc;
	merge bc states;
run;


/* Step 1.10. merge above dataset with number of firms per year */
/* proc sql;
	create table bc1 as
	select distinct a.*, b.nfirms
	from bc1 as a left join firms as b
	on a.code=b.incorp and a.year=b.calyear;
quit; */




/* Step 4. Abadie-Imbens matching: the year before Hedge Funds file 13D */

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
*final file for treatment group;

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
*final file for control group;

%mend ps_matching;



*Step 4.1. create dataset for matching;
proc sql noprint;
        select count(*) into :nrow from bc;
quit;

*%put &nrow;

%let _sdtm=%sysfunc(datetime());

options mcompilenote=ALL;
options SYMBOLGEN MPRINT MLOGIC;

%macro doit1;
%do i=1 %to &nrow;

*Reading values from a dataset;
proc sql noprint;
        select code into :state from bc where n=&i;
quit;

proc sql noprint;
        select incorp into :match from bc where n=&i;
quit;

proc sql noprint;
        select year_1 into :year from bc where n=&i;
quit;

data sample;
	set comp_extract1;
	if calyear=&year;
	if incorp="%trim(&state)" or incorp="%trim(&match)";
	if incorp="%trim(&state)" then target=1;
	else target=0;
run;

%ps_matching(data=sample,y=roa,w=target,x=size value mom,M=1,Link=logit,L=2,Lt=1);

data closestjmt;
	set closestjmt;
	year=%trim(&year)+1;
run;

data closestjmc;
	set closestjmc;
	year=%trim(&year)+1;
run;

proc append data=closestjmt base=treated_final force; run;
proc append data=closestjmc base=control_final force; run;

%end;
%mend doit1;
%doit1


%let _edtm=%sysfunc(datetime());
%let _runtm=%sysfunc(putn(&_edtm - &_sdtm, 12.4));
%put It took &_runtm second to run the program;


/* Step 4.2. create +/- five years dataset */
 data treated_final1;
	set treated_final;
	year1=year+1;
	year2=year+2;
	year3=year+3;
	year4=year+4;
	year5=year+5;
	year_1=year-1;
	year_2=year-2;
	year_3=year-3;
	year_4=year-4;
	year_5=year-5;
run;

 data control_final1;
	set control_final;
	year1=year+1;
	year2=year+2;
	year3=year+3;
	year4=year+4;
	year5=year+5;
	year_1=year-1;
	year_2=year-2;
	year_3=year-3;
	year_4=year-4;
	year_5=year-5;
run;

/* Step 4.3. Include fundamentals data in matched data for all leads and lags*/

proc sql;
	create table treated_final1 as
	select distinct a.*, b.ROA, b.CAPX, b.PPEG, b.AG, b.CH, b.SGA, b.Leverage
	from treated_final1 as a left join comp_extract1 as b
	on a.gvkeyt=b.gvkey and a.year=b.calyear;
quit;

proc sql;
	create table treated_final1 as
	select distinct a.*, b.ROA as ROA_1, b.capx as CAPX_1, b.PPEG as PPEG_1, b.AG as AG_1, b.CH as CH_1, b.SGA as SGA_1, b.Leverage as Leverage_1
	from treated_final1 as a left join comp_extract1 as b
	on a.gvkeyt=b.gvkey and a.year=b.calyear+1;
quit;

proc sql;
	create table treated_final1 as
	select distinct a.*, b.ROA as ROA_2, b.capx as CAPX_2, b.PPEG as PPEG_2, b.AG as AG_2, b.CH as CH_2, b.SGA as SGA_2, b.Leverage as Leverage_2
	from treated_final1 as a left join comp_extract1 as b
	on a.gvkeyt=b.gvkey and a.year=b.calyear+2;
quit;

proc sql;
	create table treated_final1 as
	select distinct a.*, b.ROA as ROA_3, b.capx as CAPX_3, b.PPEG as PPEG_3, b.AG as AG_3, b.CH as CH_3, b.SGA as SGA_3, b.Leverage as Leverage_3
	from treated_final1 as a left join comp_extract1 as b
	on a.gvkeyt=b.gvkey and a.year=b.calyear+3;
quit;

proc sql;
	create table treated_final1 as
	select distinct a.*, b.ROA as ROA_4, b.capx as CAPX_4, b.PPEG as PPEG_4, b.AG as AG_4, b.CH as CH_4, b.SGA as SGA_4, b.Leverage as Leverage_4
	from treated_final1 as a left join comp_extract1 as b
	on a.gvkeyt=b.gvkey and a.year=b.calyear+4;
quit;

proc sql;
	create table treated_final1 as
	select distinct a.*, b.ROA as ROA_5, b.capx as CAPX_5, b.PPEG as PPEG_5, b.AG as AG_5, b.CH as CH_5, b.SGA as SGA_5, b.Leverage as Leverage_5
	from treated_final1 as a left join comp_extract1 as b
	on a.gvkeyt=b.gvkey and a.year=b.calyear+5;
quit;

proc sql;
	create table treated_final1 as
	select distinct a.*, b.ROA as ROA1, b.capx as CAPX1, b.PPEG as PPEG1, b.AG as AG1, b.CH as CH1, b.SGA as SGA1, b.Leverage as Leverage1
	from treated_final1 as a left join comp_extract1 as b
	on a.gvkeyt=b.gvkey and a.year=b.calyear-1;
quit;

proc sql;
	create table treated_final1 as
	select distinct a.*, b.ROA as ROA2, b.capx as CAPX2, b.PPEG as PPEG2, b.AG as AG2, b.CH as CH2, b.SGA as SGA2, b.Leverage as Leverage2
	from treated_final1 as a left join comp_extract1 as b
	on a.gvkeyt=b.gvkey and a.year=b.calyear-2;
quit;

proc sql;
	create table treated_final1 as
	select distinct a.*, b.ROA as ROA3, b.capx as CAPX3, b.PPEG as PPEG3, b.AG as AG3, b.CH as CH3, b.SGA as SGA3, b.Leverage as Leverage3
	from treated_final1 as a left join comp_extract1 as b
	on a.gvkeyt=b.gvkey and a.year=b.calyear-3;
quit;

proc sql;
	create table treated_final1 as
	select distinct a.*, b.ROA as ROA4, b.capx as CAPX4, b.PPEG as PPEG4, b.AG as AG4, b.CH as CH4, b.SGA as SGA4, b.Leverage as Leverage4
	from treated_final1 as a left join comp_extract1 as b
	on a.gvkeyt=b.gvkey and a.year=b.calyear-4;
quit;

proc sql;
	create table treated_final1 as
	select distinct a.*, b.ROA as ROA5, b.capx as CAPX5, b.PPEG as PPEG5, b.AG as AG5, b.CH as CH5, b.SGA as SGA5, b.Leverage as Leverage5
	from treated_final1 as a left join comp_extract1 as b
	on a.gvkeyt=b.gvkey and a.year=b.calyear-5;
quit;


****************************************************
*Repeat for control group;
proc sql;
	create table control_final1 as
	select distinct a.*, b.ROA, b.CAPX, b.PPEG, b.AG, b.CH, b.SGA, b.Leverage
	from control_final1 as a left join comp_extract1 as b
	on a.gvkeyc=b.gvkey and a.year=b.calyear;
quit;

proc sql;
	create table control_final1 as
	select distinct a.*, b.ROA as ROA_1, b.capx as CAPX_1, b.PPEG as PPEG_1, b.AG as AG_1, b.CH as CH_1, b.SGA as SGA_1, b.Leverage as Leverage_1
	from control_final1 as a left join comp_extract1 as b
	on a.gvkeyc=b.gvkey and a.year=b.calyear+1;
quit;

proc sql;
	create table control_final1 as
	select distinct a.*, b.ROA as ROA_2, b.capx as CAPX_2, b.PPEG as PPEG_2, b.AG as AG_2, b.CH as CH_2, b.SGA as SGA_2, b.Leverage as Leverage_2
	from control_final1 as a left join comp_extract1 as b
	on a.gvkeyc=b.gvkey and a.year=b.calyear+2;
quit;

proc sql;
	create table control_final1 as
	select distinct a.*, b.ROA as ROA_3, b.capx as CAPX_3, b.PPEG as PPEG_3, b.AG as AG_3, b.CH as CH_3, b.SGA as SGA_3, b.Leverage as Leverage_3
	from control_final1 as a left join comp_extract1 as b
	on a.gvkeyc=b.gvkey and a.year=b.calyear+3;
quit;

proc sql;
	create table control_final1 as
	select distinct a.*, b.ROA as ROA_4, b.capx as CAPX_4, b.PPEG as PPEG_4, b.AG as AG_4, b.CH as CH_4, b.SGA as SGA_4, b.Leverage as Leverage_4
	from control_final1 as a left join comp_extract1 as b
	on a.gvkeyc=b.gvkey and a.year=b.calyear+4;
quit;

proc sql;
	create table control_final1 as
	select distinct a.*, b.ROA as ROA_5, b.capx as CAPX_5, b.PPEG as PPEG_5, b.AG as AG_5, b.CH as CH_5, b.SGA as SGA_5, b.Leverage as Leverage_5
	from control_final1 as a left join comp_extract1 as b
	on a.gvkeyc=b.gvkey and a.year=b.calyear+5;
quit;

proc sql;
	create table control_final1 as
	select distinct a.*, b.ROA as ROA1, b.capx as CAPX1, b.PPEG as PPEG1, b.AG as AG1, b.CH as CH1, b.SGA as SGA1, b.Leverage as Leverage1
	from control_final1 as a left join comp_extract1 as b
	on a.gvkeyc=b.gvkey and a.year=b.calyear-1;
quit;

proc sql;
	create table control_final1 as
	select distinct a.*, b.ROA as ROA2, b.capx as CAPX2, b.PPEG as PPEG2, b.AG as AG2, b.CH as CH2, b.SGA as SGA2, b.Leverage as Leverage2
	from control_final1 as a left join comp_extract1 as b
	on a.gvkeyc=b.gvkey and a.year=b.calyear-2;
quit;

proc sql;
	create table control_final1 as
	select distinct a.*, b.ROA as ROA3, b.capx as CAPX3, b.PPEG as PPEG3, b.AG as AG3, b.CH as CH3, b.SGA as SGA3, b.Leverage as Leverage3
	from control_final1 as a left join comp_extract1 as b
	on a.gvkeyc=b.gvkey and a.year=b.calyear-3;
quit;

proc sql;
	create table control_final1 as
	select distinct a.*, b.ROA as ROA4, b.capx as CAPX4, b.PPEG as PPEG4, b.AG as AG4, b.CH as CH4, b.SGA as SGA4, b.Leverage as Leverage4
	from control_final1 as a left join comp_extract1 as b
	on a.gvkeyc=b.gvkey and a.year=b.calyear-4;
quit;

proc sql;
	create table control_final1 as
	select distinct a.*, b.ROA as ROA5, b.capx as CAPX5, b.PPEG as PPEG5, b.AG as AG5, b.CH as CH5, b.SGA as SGA5, b.Leverage as Leverage5
	from control_final1 as a left join comp_extract1 as b
	on a.gvkeyc=b.gvkey and a.year=b.calyear-5;
quit;


/* Step 4.4. Estimate means for trearment and control groups by year*/
proc means data=treated_final1 noprint;
  by target;
  var roa_5 roa_4 roa_3 roa_2 roa_1 roa roa1 roa2 roa3 roa4 roa5;
  output out=test1t mean=L_5 L_4 L_3 L_2 L_1 L L1 L2 L3 L4 L5;
quit;

proc means data=control_final1 noprint;
  by target;
  var roa_5 roa_4 roa_3 roa_2 roa_1 roa roa1 roa2 roa3 roa4 roa5;
  output out=test1c mean=L_5 L_4 L_3 L_2 L_1 L L1 L2 L3 L4 L5;
quit;

proc means data=treated_final1 noprint;
  by target;
  var capx_5 capx_4 capx_3 capx_2 capx_1 capx capx1 capx2 capx3 capx4 capx5;
  output out=test2t mean=L_5 L_4 L_3 L_2 L_1 L L1 L2 L3 L4 L5;
quit;

proc means data=control_final1 noprint;
  by target;
  var capx_5 capx_4 capx_3 capx_2 capx_1 capx capx1 capx2 capx3 capx4 capx5;
  output out=test2c mean=L_5 L_4 L_3 L_2 L_1 L L1 L2 L3 L4 L5;
quit;

proc means data=treated_final1 noprint;
  by target;
  var PPEG_5 PPEG_4 PPEG_3 PPEG_2 PPEG_1 PPEG PPEG1 PPEG2 PPEG3 PPEG4 PPEG5;
  output out=test3t mean=L_5 L_4 L_3 L_2 L_1 L L1 L2 L3 L4 L5;
quit;

proc means data=control_final1 noprint;
  by target;
  var PPEG_5 PPEG_4 PPEG_3 PPEG_2 PPEG_1 PPEG PPEG1 PPEG2 PPEG3 PPEG4 PPEG5;
  output out=test3c mean=L_5 L_4 L_3 L_2 L_1 L L1 L2 L3 L4 L5;
quit;

proc means data=treated_final1 noprint;
  by target;
  var AG_5 AG_4 AG_3 AG_2 AG_1 AG AG1 AG2 AG3 AG4 AG5;
  output out=test4t mean=L_5 L_4 L_3 L_2 L_1 L L1 L2 L3 L4 L5;
quit;

proc means data=control_final1 noprint;
  by target;
  var AG_5 AG_4 AG_3 AG_2 AG_1 AG AG1 AG2 AG3 AG4 AG5;
  output out=test4c mean=L_5 L_4 L_3 L_2 L_1 L L1 L2 L3 L4 L5;
quit;

proc means data=treated_final1 noprint;
  by target;
  var CH_5 CH_4 CH_3 CH_2 CH_1 CH CH1 CH2 CH3 CH4 CH5;
  output out=test5t mean=L_5 L_4 L_3 L_2 L_1 L L1 L2 L3 L4 L5;
quit;

proc means data=control_final1 noprint;
  by target;
  var CH_5 CH_4 CH_3 CH_2 CH_1 CH CH1 CH2 CH3 CH4 CH5;
  output out=test5c mean=L_5 L_4 L_3 L_2 L_1 L L1 L2 L3 L4 L5;
quit;

proc means data=treated_final1 noprint;
  by target;
  var SGA_5 SGA_4 SGA_3 SGA_2 SGA_1 SGA SGA1 SGA2 SGA3 SGA4 SGA5;
  output out=test6t mean=L_5 L_4 L_3 L_2 L_1 L L1 L2 L3 L4 L5;
quit;

proc means data=control_final1 noprint;
  by target;
  var SGA_5 SGA_4 SGA_3 SGA_2 SGA_1 SGA SGA1 SGA2 SGA3 SGA4 SGA5;
  output out=test6c mean=L_5 L_4 L_3 L_2 L_1 L L1 L2 L3 L4 L5;
quit;

proc means data=treated_final1 noprint;
  by target;
  var LEVERAGE_5 LEVERAGE_4 LEVERAGE_3 LEVERAGE_2 LEVERAGE_1 LEVERAGE LEVERAGE1 LEVERAGE2 LEVERAGE3 LEVERAGE4 LEVERAGE5;
  output out=test7t mean=L_5 L_4 L_3 L_2 L_1 L L1 L2 L3 L4 L5;
quit;

proc means data=control_final1 noprint;
  by target;
  var LEVERAGE_5 LEVERAGE_4 LEVERAGE_3 LEVERAGE_2 LEVERAGE_1 LEVERAGE LEVERAGE1 LEVERAGE2 LEVERAGE3 LEVERAGE4 LEVERAGE5;
  output out=test7c mean=L_5 L_4 L_3 L_2 L_1 L L1 L2 L3 L4 L5;
quit;


proc append data=test1t base=plot force; run;
proc append data=test1c base=plot force; run;
proc append data=test2t base=plot force; run;
proc append data=test2c base=plot force; run;
proc append data=test3t base=plot force; run;
proc append data=test3c base=plot force; run;
proc append data=test4t base=plot force; run;
proc append data=test4c base=plot force; run;
proc append data=test5t base=plot force; run;
proc append data=test5c base=plot force; run;
proc append data=test6t base=plot force; run;
proc append data=test6c base=plot force; run;
proc append data=test7t base=plot force; run;
proc append data=test7c base=plot force; run;

*Export csv;
proc export data=plot outfile="C:\Users\KUTHURU\Downloads\Laptop\Semester 3\Corporate Finance Slava\Homework-4\plot.csv" 
dbms=csv replace;
run;
