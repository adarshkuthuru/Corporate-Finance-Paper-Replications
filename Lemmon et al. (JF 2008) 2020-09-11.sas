proc datasets lib=work kill nolist memtype=data;
quit;

libname CF 'C:\Users\KUTHURU\Downloads\Semester 3\Corp Fin Slava\Homework-1'; run;


%let wrds=wrds.wharton.upenn.edu 4016; 
options comamid=TCP remote=WRDS;        
signon username=_prompt_;               
                                         
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


*Specify CRSP begin and end dates;
%let crspbegdate = '01Jan1963'd;
%let crspenddate = '31DEC2003'd; 

endrsubmit;



/***************************************************/
/*    Step 1. Obtain data and summary stats  */
/***************************************************/

rsubmit;
/* Step 1.1. Obtain Data from Compustat */
data comp_extract;
   format gvkey datadate date_fyend calyear fyear fyr indfmt consol datafmt popsrc sich dlc dltt at prcc_f cshpri
	pstkl txditc invt sale oibdp ppent intan dvc;  /*http://www.crsp.com/products/documentation/annual-data-industrial */
   set compa.funda
   (where=(fyr>0 and at>0 and consol='C' and
           indfmt='INDL' and datafmt='STD' and popsrc='D'));

   *Converts fiscal year into calendar year data;
/*   if (1<=fyr<=5) then date_fyend=intnx('month',mdy(fyr,1,fyear+1),0,'end');*/
/*   else if (6<=fyr<=12) then date_fyend=intnx('month',mdy(fyr,1,fyear),0,'end');*/
/*   calyear=year(date_fyend);*/
/*   format date_fyend date9.;*/

   if missing(at)=0;
   if year(&crspbegdate)<=year(datadate)<=year(&crspenddate);
   format datadate date9.;
   keep gvkey datadate calyear fyear fyr indfmt consol datafmt popsrc sich dlc dltt at prcc_f cshpri
	pstkl txditc invt sale oibdp ppent intan dvc;
   *Note that datadate is same as date_fyend;
run;
*294892 obs; 

data comp_extract;
	set comp_extract;
	start_date=&crspbegdate;
	format start_date date9.;
run;

/* Step 1.2. Past three years operating income data to estimate Cash Flow Volatility 
proc sql;
	create table comp_extract as 
	select distinct a.*,b.oibdp as oibdp_1
	from comp_extract as a left join compa.funda as b
	on a.gvkey=b.gvkey and a.fyear=b.fyear+1 and missing(b.oibdp)=0;
quit; */

*First observation date by each firm;
/*proc sql;
  create table comp_extract1 as
  select distinct a.*
  from comp_extract as a left join comp_extract as b
  on a.gvkey=b.gvkey and a.start_date<b.Datadate<=a.Datadate and missing(b.oibdp)=0
  group by a.gvkey, a.Datadate
  having count(distinct b.Datadate)>2; *Require good data for at least 3 months;
quit; */
*239060 obs;

proc sql;
  create table comp_extract2 as
  select distinct a.*, std(b.oibdp) as Cash_flow_vol
  from comp_extract as a left join comp_extract as b
  on a.gvkey=b.gvkey and a.start_date<b.Datadate<=intnx("month",a.Datadate,0,"E") and missing(b.oibdp)=0
  group by a.gvkey, a.Datadate
  having count(distinct b.Datadate)>2; *Require good data for at least 3 months;
quit;
*193984 obs;

/* Step 1.3. Estimate variables */
data comp_extract;
	set comp_extract;
	Total_Debt=sum(dltt,dlc);*dltt, dlc in millions;
	Book_Leverage=sum(dltt,dlc)/at; *at in millions;
	Log_sales=log(sale); *sale in millions;
	ME=abs(prcc_f)*cshpri;*cshpri in millions;
	Mkt_leverage=sum(dltt,dlc)/sum(dltt,dlc,abs(prcc_f)*cshpri);
	Mkt_book=sum(abs(prcc_f)*cshpri,dltt,dlc,pstkl,-txditc)/at; *dltt, dlc, cshpri, pstkl, txditc in millions;
	Profitability=oibdp/at; *oibdp in millions;
	Tangibility=ppent/at; *ppent in millions;
	if missing(dvc)=1 or dvc=0 then div_payer=0;
	else div_payer=1;
	intangible_assets=sum(at,-ppent); *intan in millions;
run; 
*294892 obs;

*Add cash flow volatility to the main dataset;
proc sql;
	create table comp_extract as
	select distinct a.*, b.Cash_flow_vol
	from comp_extract as a left join comp_extract2 as b
	on a.gvkey=b.gvkey and a.datadate=b.datadate;
quit;
*294892 obs;

data temp.comp_extract;
	set comp_extract;
run;
endrsubmit;


/* Step 1.3. Requires market and book leverage to be less than unit and greater than 0*/

data cf.comp_extract;
	set cf.comp_extract;
	if datadate>'01JAN1965'd;
	if Book_Leverage>1 or Book_Leverage<0 then delete;
	if Mkt_leverage>1 or Mkt_leverage<0 then delete;
run; 
*274380 obs;

/*  proc sql;
	create table cf.test as
	select distinct median(Book_Leverage) as median_Book_Leverage, std(Book_Leverage) as std_Book_Leverage,
	median(Mkt_Leverage) as median_Mkt_Leverage, std(Mkt_leverage) as Std_Mkt_leverage
	from cf.comp_extract;
quit; */



/* %let L=10;    %* 10th percentile *;
%let H=%eval(100 - &L);   %* 90th percentile*;
proc univariate data=cf.comp_extract noprint;
   var Log_sales Mkt_book Profitability Tangibility Cash_flow_vol intangible_assets;
   output out=_winsor   pctlpts=&L  &H    
   pctlpre=__Log_sales  __Mkt_book  __Profitability __Tangibility __Cash_flow_vol __intangible_assets;
run;
data cf.comp_extract1 (drop=__:);
  set cf.comp_extract;
  if _n_=1 then set _winsor;
  array wlo  {*} __Log_sales&L  __Mkt_book&L   __Profitability&L __Tangibility&L __Cash_flow_vol&L __intangible_assets&L;
  array whi  {*} __Log_sales&H __Mkt_book&H __Profitability&H __Tangibility&H __Cash_flow_vol&H __intangible_assets&H;
  array wval {*} wLog_sales wMkt_book wProfitability wTangibility wCash_flow_vol wintangible_assets;
  array val   {*} Log_sales Mkt_book Profitability Tangibility Cash_flow_vol intangible_assets;
  do _V=1 to dim(val);
     wval{_V}=min(max(val{_V},wlo{_V}),whi{_V});
  end;
run; */


/* Step 1.4. Winsorize the variables*/

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
%winsor(dsetin=cf.comp_extract, dsetout=cf.comp_extract1, byvar=none, 
vars=Log_sales Mkt_book Profitability Tangibility Cash_flow_vol intan, type=winsor, pctl=1 99);


/* Step 1.5. Fama-French Industry clssification*/

%macro ff38(dsin=, dsout=, sicvar=sich, varname=ff38);

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
%ff38(dsin=cf.comp_extract1, dsout=cf.comp_extract1, varname=ff38);

/* Step 1.6. Summary Stats for full sample*/
proc sql;
	create table beta1 as
	select distinct mean(Book_Leverage) as mean, median(Book_Leverage) as median, std(Book_Leverage) as std
	from cf.comp_extract1;
quit;
data beta1; format stat; set beta1;stat='Book Leverage'; run;

proc sql;
	create table beta2 as
	select distinct mean(Mkt_Leverage) as mean, median(Mkt_Leverage) as median, std(Mkt_Leverage) as std
	from cf.comp_extract1;
quit;
data beta2; format stat; set beta2;stat='Market Leverage'; run;

proc sql;
	create table beta3 as
	select distinct mean(Log_sales) as mean, median(Log_sales) as median, std(Log_sales) as std
	from cf.comp_extract1;
quit;
data beta3; format stat; set beta3;stat='Log sales'; run;

proc sql;
	create table beta4 as
	select distinct mean(Mkt_book) as mean, median(Mkt_book) as median, std(Mkt_book) as std
	from cf.comp_extract1;
quit;
data beta4; format stat; set beta4;stat='Market to book'; run;

proc sql;
	create table beta5 as
	select distinct mean(Profitability) as mean, median(Profitability) as median, std(Profitability) as std
	from cf.comp_extract1;
quit;
data beta5; format stat; set beta5; stat='Profitability'; run;

proc sql;
	create table beta6 as
	select distinct mean(Tangibility) as mean, median(Tangibility) as median, std(Tangibility) as std
	from cf.comp_extract1;
quit;
data beta6; format stat; set beta6; stat='Tangibility'; run;

proc sql;
	create table beta7 as
	select distinct mean(Cash_flow_vol) as mean, median(Cash_flow_vol) as median, std(Cash_flow_vol) as std
	from cf.comp_extract1;
quit;
data beta7; format stat; set beta7; stat='Cash flow volatility'; run;

proc sql;
	create table beta8 as
	select distinct ff38, median(book_leverage) as median_industry
	from cf.comp_extract1
	group by ff38;
quit;
*add variable to main dataset;
proc sql;
	create table cf.comp_extract1 as
	select distinct a.*, b.median_industry
	from cf.comp_extract1 as a left join beta8 as b
	on a.ff38=b.ff38;
quit;
proc sql;
	create table beta8 as
	select distinct mean(median_industry) as mean, median(median_industry) as median, std(median_industry) as std
	from beta8;
quit;
data beta8; format stat; set beta8; stat='median industry book leverage'; run;

proc sql;
	create table beta9 as
	select distinct mean(div_payer) as mean, median(div_payer) as median, std(div_payer) as std
	from cf.comp_extract1;
quit;
data beta9; format stat; set beta9; stat='dividend payer'; run;

proc sql;
	create table beta10 as
	select distinct mean(intan) as mean, median(intan) as median, std(intan) as std
	from cf.comp_extract1;
quit;
data beta10; format stat; set beta10; stat='Intangible assets'; run;




proc append data=beta1 base=result; run;
proc append data=beta2 base=result force; run;
proc append data=beta3 base=result force; run;
proc append data=beta4 base=result force; run;
proc append data=beta5 base=result force; run;
proc append data=beta6 base=result force; run;
proc append data=beta7 base=result force; run;
proc append data=beta8 base=result force; run;
proc append data=beta9 base=result force; run;
proc append data=beta10 base=result force; run;


/* Step 1.7. Summary Stats for survivors*/
proc sql;
	create table cf.all_firms as
	select distinct gvkey, count(distinct datadate) as ncount
	from cf.comp_extract1
	group by gvkey;
quit;
*26049 obs;

proc sql;
	create table cf.survivors as
	select distinct gvkey, count(distinct datadate) as ncount
	from cf.comp_extract1
	group by gvkey
	having count(distinct datadate)>19;
quit;
*3769 obs;

proc sql;
	create table cf.comp_extract2 as
	select distinct a.* 
	from cf.comp_extract1 as a, cf.survivors as b
	where a.gvkey=b.gvkey;
quit;
*109088 obs;

proc sql;
	create table beta1 as
	select distinct mean(Book_Leverage) as mean, median(Book_Leverage) as median, std(Book_Leverage) as std
	from cf.comp_extract2;
quit;
data beta1; format stat; set beta1;stat='Book Leverage'; run;

proc sql;
	create table beta2 as
	select distinct mean(Mkt_Leverage) as mean, median(Mkt_Leverage) as median, std(Mkt_Leverage) as std
	from cf.comp_extract2;
quit;
data beta2; format stat; set beta2;stat='Market Leverage'; run;

proc sql;
	create table beta3 as
	select distinct mean(Log_sales) as mean, median(Log_sales) as median, std(Log_sales) as std
	from cf.comp_extract2;
quit;
data beta3; format stat; set beta3;stat='Log sales'; run;

proc sql;
	create table beta4 as
	select distinct mean(Mkt_book) as mean, median(Mkt_book) as median, std(Mkt_book) as std
	from cf.comp_extract2;
quit;
data beta4; format stat; set beta4;stat='Market to book'; run;

proc sql;
	create table beta5 as
	select distinct mean(Profitability) as mean, median(Profitability) as median, std(Profitability) as std
	from cf.comp_extract2;
quit;
data beta5; format stat; set beta5; stat='Profitability'; run;

proc sql;
	create table beta6 as
	select distinct mean(Tangibility) as mean, median(Tangibility) as median, std(Tangibility) as std
	from cf.comp_extract2;
quit;
data beta6; format stat; set beta6; stat='Tangibility'; run;

proc sql;
	create table beta7 as
	select distinct mean(Cash_flow_vol) as mean, median(Cash_flow_vol) as median, std(Cash_flow_vol) as std
	from cf.comp_extract2;
quit;
data beta7; format stat; set beta7; stat='Cash flow volatility'; run;

proc sql;
	create table beta8 as
	select distinct ff38, median(book_leverage) as median_industry
	from cf.comp_extract2
	group by ff38;
quit;
*add variable to main dataset;
proc sql;
	create table cf.comp_extract2 as
	select distinct a.*, b.median_industry
	from cf.comp_extract2 as a left join beta8 as b
	on a.ff38=b.ff38;
quit;
proc sql;
	create table beta8 as
	select distinct mean(median_industry) as mean, median(median_industry) as median, std(median_industry) as std
	from beta8;
quit;
data beta8; format stat; set beta8; stat='median industry book leverage'; run;

proc sql;
	create table beta9 as
	select distinct mean(div_payer) as mean, median(div_payer) as median, std(div_payer) as std
	from cf.comp_extract2;
quit;
data beta9; format stat; set beta9; stat='dividend payer'; run;

proc sql;
	create table beta10 as
	select distinct mean(intan) as mean, median(intan) as median, std(intan) as std
	from cf.comp_extract2;
quit;
data beta10; format stat; set beta10; stat='Intangible assets'; run;




proc append data=beta1 base=result2; run;
proc append data=beta2 base=result2 force; run;
proc append data=beta3 base=result2 force; run;
proc append data=beta4 base=result2 force; run;
proc append data=beta5 base=result2 force; run;
proc append data=beta6 base=result2 force; run;
proc append data=beta7 base=result2 force; run;
proc append data=beta8 base=result2 force; run;
proc append data=beta9 base=result2 force; run;
proc append data=beta10 base=result2 force; run;



/* Step 1.8. Regressions-Table 3 */

*Step 1.8.1. create a random sample which contains 10% of the firms from full sample; 
PROC SURVEYSELECT DATA=cf.all_firms noprint OUT=random METHOD=SRS
SAMPSIZE=2500 SEED=26049;
RUN;
*2500 obs;

Proc sql;
	create table cf.comp_extract_random as
	select distinct a.*
	from cf.comp_extract1 as a, random as b
	where a.gvkey=b.gvkey;
quit;
*26445 obs;

* Step 1.8.2 Variance decomoposition models;
*(i)Book Leverage as dep variable;

*(c);
proc glm data = cf.comp_extract_random noprint outstat=glm;
 class gvkey fyear;
 model Book_Leverage = gvkey fyear/ss3 noint; run;
quit;
data glm;
	set glm;
	if _TYPE_='SS3';
	keep _TYPE_ _SOURCE_ ss;
run;
proc sql;
	create table glm as
	select distinct *,ss/sum(ss) as var
	from glm 
	group by _type_;
quit;


*(d);
proc glm data = cf.comp_extract_random noprint outstat=glm1;
 class fyear ff38;
 model Book_Leverage = fyear log_sales Mkt_book Profitability Tangibility ff38/solution noint; run;
quit;
data glm1;
	set glm1;
	if _TYPE_='SS3';
	keep _TYPE_ _SOURCE_ ss;
run;
proc sql;
	create table glm1 as
	select distinct *,ss/sum(ss) as var
	from glm1 
	group by _type_;
quit;

*(e);
proc glm data = cf.comp_extract_random noprint outstat=glm2;
 class gvkey fyear;
 model Book_Leverage = gvkey fyear log_sales Mkt_book Profitability Tangibility/ss3 noint; run;
quit;
data glm2;
	set glm2;
	if _TYPE_='SS3';
	keep _TYPE_ _SOURCE_ ss;
run;
proc sql;
	create table glm2 as
	select distinct *,ss/sum(ss) as var
	from glm2 
	group by _type_;
quit;

*(f);
proc glm data = cf.comp_extract_random noprint outstat=glm3;
 class fyear ff38;
 model Book_Leverage = fyear log_sales Mkt_book Profitability Tangibility median_industry Cash_flow_vol div_payer ff38/ss3 noint; run;
quit;
data glm3;
	set glm3;
	if _TYPE_='SS3';
	keep _TYPE_ _SOURCE_ ss;
run;
proc sql;
	create table glm3 as
	select distinct *,ss/sum(ss) as var
	from glm3 
	group by _type_;
quit;

*(g);
proc glm data = cf.comp_extract_random noprint outstat=glm4;
 class gvkey fyear;
 model Book_Leverage = gvkey fyear log_sales Mkt_book Profitability Tangibility median_industry Cash_flow_vol div_payer/ss3 noint; run;
quit;
data glm4;
	set glm4;
	if _TYPE_='SS3';
	keep _TYPE_ _SOURCE_ ss;
run;
proc sql;
	create table glm4 as
	select distinct *,ss/sum(ss) as var
	from glm4 
	group by _type_;
quit;

*concatenate above tables;
data result3;
	input _SOURCE_ $20.;
	datalines;
	gvkey
	fyear
	log_sales 
	Mkt_book 
	Profitability 
	Tangibility 
	median_industry 
	Cash_flow_vol 
	div_payer
	ff38
	;
proc sql;
	create table result3 as
	select distinct a._SOURCE_, b.var
	from result3 as a left join glm as b
	on a._SOURCE_=b._SOURCE_;
quit;
proc sql;
	create table result3 as
	select distinct a.*, b.var as var1
	from result3 as a left join glm1 as b
	on a._SOURCE_=b._SOURCE_;
quit;
proc sql;
	create table result3 as
	select distinct a.*, b.var as var2
	from result3 as a left join glm2 as b
	on a._SOURCE_=b._SOURCE_;
quit;
proc sql;
	create table result3 as
	select distinct a.*, b.var as var3
	from result3 as a left join glm3 as b
	on a._SOURCE_=b._SOURCE_;
quit;
proc sql;
	create table result3 as
	select distinct a.*, b.var as var4
	from result3 as a left join glm4 as b
	on a._SOURCE_=b._SOURCE_;
quit;


*(ii)Market Leverage as dep variable;

*(c);
proc glm data = cf.comp_extract_random noprint outstat=glm;
 class gvkey fyear;
 model Mkt_Leverage = gvkey fyear/ss3 noint; run;
quit;
data glm;
	set glm;
	if _TYPE_='SS3';
	keep _TYPE_ _SOURCE_ ss;
run;
proc sql;
	create table glm as
	select distinct *,ss/sum(ss) as var
	from glm 
	group by _type_;
quit;


*(d);
proc glm data = cf.comp_extract_random noprint outstat=glm1;
 class fyear ff38;
 model Mkt_Leverage = fyear log_sales Mkt_book Profitability Tangibility ff38/solution noint; run;
quit;
data glm1;
	set glm1;
	if _TYPE_='SS3';
	keep _TYPE_ _SOURCE_ ss;
run;
proc sql;
	create table glm1 as
	select distinct *,ss/sum(ss) as var
	from glm1 
	group by _type_;
quit;

*(e);
proc glm data = cf.comp_extract_random noprint outstat=glm2;
 class gvkey fyear;
 model Mkt_Leverage = gvkey fyear log_sales Mkt_book Profitability Tangibility/ss3 noint; run;
quit;
data glm2;
	set glm2;
	if _TYPE_='SS3';
	keep _TYPE_ _SOURCE_ ss;
run;
proc sql;
	create table glm2 as
	select distinct *,ss/sum(ss) as var
	from glm2 
	group by _type_;
quit;

*(f);
proc glm data = cf.comp_extract_random noprint outstat=glm3;
 class fyear ff38;
 model Mkt_Leverage = fyear log_sales Mkt_book Profitability Tangibility median_industry Cash_flow_vol div_payer ff38/ss3 noint; run;
quit;
data glm3;
	set glm3;
	if _TYPE_='SS3';
	keep _TYPE_ _SOURCE_ ss;
run;
proc sql;
	create table glm3 as
	select distinct *,ss/sum(ss) as var
	from glm3 
	group by _type_;
quit;

*(g);
proc glm data = cf.comp_extract_random noprint outstat=glm4;
 class gvkey fyear;
 model Mkt_Leverage = gvkey fyear log_sales Mkt_book Profitability Tangibility median_industry Cash_flow_vol div_payer/ss3 noint; run;
quit;
data glm4;
	set glm4;
	if _TYPE_='SS3';
	keep _TYPE_ _SOURCE_ ss;
run;
proc sql;
	create table glm4 as
	select distinct *,ss/sum(ss) as var
	from glm4 
	group by _type_;
quit;

*concatenate above tables;
data result4;
	input _SOURCE_ $20.;
	datalines;
	gvkey
	fyear
	log_sales 
	Mkt_book 
	Profitability 
	Tangibility 
	median_industry 
	Cash_flow_vol 
	div_payer
	ff38
	;
proc sql;
	create table result4 as
	select distinct a._SOURCE_, b.var
	from result4 as a left join glm as b
	on a._SOURCE_=b._SOURCE_;
quit;
proc sql;
	create table result4 as
	select distinct a.*, b.var as var1
	from result4 as a left join glm1 as b
	on a._SOURCE_=b._SOURCE_;
quit;
proc sql;
	create table result4 as
	select distinct a.*, b.var as var2
	from result4 as a left join glm2 as b
	on a._SOURCE_=b._SOURCE_;
quit;
proc sql;
	create table result4 as
	select distinct a.*, b.var as var3
	from result4 as a left join glm3 as b
	on a._SOURCE_=b._SOURCE_;
quit;
proc sql;
	create table result4 as
	select distinct a.*, b.var as var4
	from result4 as a left join glm4 as b
	on a._SOURCE_=b._SOURCE_;
quit;

/* Step 1.9. Regressions-Table 5 */

*Pooled regressions;
proc sort data=MonthlyStockData5_Port; by AnomalyVar Rank_Anomaly; run;
proc reg data=MonthlyStockData5_Port noprint tableout outest=Alpha;
  by AnomalyVar Rank_Anomaly;
  model ExVWHoldRet = MKTRF;
quit;
data Alpha;
  set Alpha;
  where _Type_ in ('PARMS','T');
  keep AnomalyVar Rank_Anomaly _Type_ Intercept;
  if _Type_='PARMS' then Intercept=Intercept*12*100;
  rename Intercept=VWHoldRet;
  rename _Type_ =Stat;
run;
