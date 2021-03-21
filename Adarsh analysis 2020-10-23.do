
cd "C:\Users\KUTHURU\Downloads\Laptop\Semester 3\Corporate Finance Slava\Homework-4"

import delimited "C:\Users\KUTHURU\Downloads\Laptop\Semester 3\Corporate Finance Slava\Homework-4\data.csv", clear
save adarsh_data, replace 

eststo clear

*drop if calyear>1996
*drop if calyear<1976

encode state, gen(state1)
*encode incorp, gen(incorp1)

egen fe1=group(state1 calyear)
egen fe2=group(sic calyear)

/* Table-4 */
reghdfe roa bc size sizesq age agesq, absorb(gvkey fe1 fe2) cluster(state1)
quietly eststo Model1

*reghdfe roa bc size sizesq age agesq, absorb(gvkey incorp1#calyear sic#calyear) cluster(incorp1)
*quietly eststo Model1


reghdfe capx bc size sizesq age agesq, absorb(gvkey fe1 fe2) cluster(state1)
quietly eststo Model2

reghdfe ppeg bc size sizesq age agesq, absorb(gvkey fe1 fe2) cluster(state1)
quietly eststo Model3

reghdfe ag bc size sizesq age agesq, absorb(gvkey fe1 fe2) cluster(state1)
quietly eststo Model4

reghdfe ch bc size sizesq age agesq, absorb(gvkey fe1 fe2) cluster(state1)
quietly eststo Model5

reghdfe sga bc size sizesq age agesq, absorb(gvkey fe1 fe2) cluster(state1)
quietly eststo Model6

reghdfe leverage bc size sizesq age agesq, absorb(gvkey fe1 fe2) cluster(state1)
quietly eststo Model7


esttab using Table4_1.csv, replace compress r2 nogaps b se(%8.3f) drop(_cons size sizesq age agesq)
order(bc)
