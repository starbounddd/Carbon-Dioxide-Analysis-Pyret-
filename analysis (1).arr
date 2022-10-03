use context essentials2021
include shared-gdrive("dcic-2021", "1wyQZj_L0qqV9Ekgr9au6RX2iqt2Ga8Ep")
include tables
include gdrive-sheets
include image
import math as M
import statistics as S
import data-source as DS
 
google-id = "1XBaxyRvMQpTec0mRVAvJsplMr2iEcGetSMpguRnU3qo" 
 
warming-deg-c-unsanitized-table = load-spreadsheet(google-id)
 
co2-emissions-unsanitized-table = load-spreadsheet(google-id)
 
years-1960-2014-unsanitized-table = load-spreadsheet(google-id)
 
 
warming-deg-c-table = load-table: country :: String, warming-deg-c-since-1960 :: String, region :: String
  source: warming-deg-c-unsanitized-table.sheet-by-name("warming-degc", true)
  sanitize country using DS.string-sanitizer
  sanitize warming-deg-c-since-1960 using DS.string-sanitizer
  sanitize region using DS.string-sanitizer
end
 
co2-emissions-table = load-table: year :: Number, country :: String, total-co2-emission-metric-ton :: Number, per-capita :: Number
  source: warming-deg-c-unsanitized-table.sheet-by-name("fossil-fuel-co2-emissions-by-nation_csv", true)
  sanitize year using DS.strict-num-sanitizer
  sanitize country using DS.string-sanitizer
  sanitize total-co2-emission-metric-ton using DS.strict-num-sanitizer
  sanitize per-capita using DS.strict-num-sanitizer
end
 
years-1960-2014-table = load-table: year :: Number
  source: years-1960-2014-unsanitized-table.sheet-by-name("years", true)
  sanitize year using DS.strict-num-sanitizer
end

#we need to make the country columns for both the warming-deg-c-table and the c02-emissions table the same. In order to do this, we have to clean up the column names and make them equivalent. To do this, we will make the country column lowercase

#clean the c02-emissions-table so that it does not have any hyphens or forward slashes

#test table 1 used for examples and checking if the country functions work
test-table = table: year :: Number, country :: String, metric-ton :: Number, capita :: Number
  row: 3000, "/america", 200, 0.2
  row: 2002, "//Mexico//", 131, 0.1
  row: 2001, "Le-bron", 500 , 0.6
  row: 2000, "Rhino", 300, 32
  row: 2005, "jetson", 21, 65
end
    
    cleaned-test-table = table: year :: Number, country :: String, metric-ton :: Number, capita :: Number
  row: 3000, "america", 200, 0.2
      row: 2002, "mexico", 131, 0.1
      row: 2001, "le bron", 500 , 0.6
      row: 2000, "rhino", 300, 32
      row: 2005, "jetson", 21, 65
end
#helper functions for cleaning table


fun remove-characters(country :: String) -> String:
  doc:"removes all / and - in the country column"
  string-replace(string-replace(country, "/", ""), "-", " ")
    where:
  remove-characters("//Bob") is "Bob"
  remove-characters("Michael Jean-Baptiste") is "Michael Jean Baptiste"
  remove-characters("//James-Bond//") is "James Bond"
  remove-characters("Cracked-&-Broken/") is "Cracked & Broken"
end
  
fun lowercase-all-countries(country :: String) -> String:
  doc: "takes in the name of a country to turn the characters into lowercase letters"
  string-to-lower(country)
where:
  lowercase-all-countries("Lewis") is "lewis"
  lowercase-all-countries("BROTHER") is "brother"
  lowercase-all-countries("PaNaMa") is "panama"
end

fun clean-table(t :: Table) -> Table:
  doc: "applies changes to table with country column"
  table-with-lowercase-countries = transform-column(t, "country", remove-characters)
  transform-column(table-with-lowercase-countries, "country", lowercase-all-countries)
        where: 
  clean-table(test-table) is cleaned-test-table
end




#tables with cleaned country column
new-co2-emissions = clean-table(co2-emissions-table)
new-warming-deg-c-table = clean-table(warming-deg-c-table)

#test-tables for make-region-table
co2-like-test-table = table: year :: Number, country :: String, total-co2-emission-metric-ton :: Number, per-capita :: Number
  row: 1000, "new york", 200, 22
  row:2000, "boston", 900, 29
  row:3000, "massachussetts", 300, 21
  row:4000, "texas", 700, 23
  row:5000, "florida", 100, 24
end


warm-like-test-table = table: country :: String, warming-deg-c-since-1960 :: Number , region :: String
  row: "new york", 100, "Narnia"
  row: "boston", 23, "Narnia"
  row: "massachussetts", 21, "China"
      row: "texas", 11, "China"
      row: "florida", 30, "Idk"
    end

narnia-region-co2-emission-sum-table =  table: year :: Number, country :: String, total-co2-emission-metric-ton :: Number, per-capita :: Number, region :: String
  row: 1000, "new york", 200, 22, "Narnia"
  row: 2000, "boston", 900, 29, "Narnia"
end
  
china-region-co2-emission-sum-table = table: year :: Number, country :: String, total-co2-emission-metric-ton :: Number, per-capita :: Number, region :: String
  row: 3000, "massachussetts", 300, 21, "China"
  row: 4000, "texas", 700, 23, "China"
end



fun make-region-tables(co2-table :: Table, warm-table :: Table, region :: String ) -> Table:
  doc: "taks in two tables similar to the co2 emissions table and the warming degrees table, as well as a string(the country name) to produce a taable that contains a table with co2 emissions and the region associated with the following country"
  
  
  #fun inside-w(warm2-table :: Table):
    
  fun full-region(genr :: Row ) -> String:
   
    #warm-country = warmr["country"]
    
    fun gt-region(warmr:: Row) -> Boolean:
    
    warmr["region"] == region
    end
    
    new-warm = filter-with(warm-table, gt-region)
    base-countries = select-columns(new-warm , [list: "country", "region"])
    warm-country = base-countries.get-column("country")
    
    # fun idk(r :: Row):
    country-co2 = genr["country"]
    
    if warm-country.member(country-co2) :
        
      base-countries.row-n(0)["region"] 
    else: " "  
      end
    #end   
  
  end
  
  # f-table = filter-with(warm-table, full-region)
    
  table-with-col = build-column(co2-table, "region", full-region)  
  
  fun gt-region2(warmr:: Row) -> Boolean:
    
    warmr["region"] == region
    end
  
  filter-with(table-with-col, gt-region2)
where:
  make-region-tables(co2-like-test-table, warm-like-test-table, "Narnia") is narnia-region-co2-emission-sum-table
  make-region-tables(co2-like-test-table, warm-like-test-table, "China") is china-region-co2-emission-sum-table
        
    end
    
  
Africa-co2 = make-region-tables(new-co2-emissions,new-warming-deg-c-table, "Africa")
Asia-co2 = make-region-tables(new-co2-emissions,new-warming-deg-c-table, "Asia")
N-America-co2 = make-region-tables(new-co2-emissions,new-warming-deg-c-table, "North America")
S-America-co2 = make-region-tables(new-co2-emissions,new-warming-deg-c-table, "South America")
Oceania-co2 = make-region-tables(new-co2-emissions,new-warming-deg-c-table, "Oceania")
Europe-co2 = make-region-tables(new-co2-emissions,new-warming-deg-c-table, "Europe")

#we don't have co2 values for the others but just incase
Other-co2 = make-region-tables(new-co2-emissions,new-warming-deg-c-table, "Other")

Africa-Co2-Sum = sum(Africa-co2, "total-co2-emission-metric-ton")
Asia-Co2-Sum= sum(Asia-co2, "total-co2-emission-metric-ton")
NA-Co2-Sum= sum(N-America-co2, "total-co2-emission-metric-ton")
SA-Co2-Sum = sum(S-America-co2, "total-co2-emission-metric-ton")
Oceania-Co2-Sum = sum(Oceania-co2, "total-co2-emission-metric-ton")
Europe-Co2-Sum = sum(Europe-co2, "total-co2-emission-metric-ton")

CO2-Emissions-Sum-Table = table: region :: String, sum :: Number
  row: "Africa", Africa-Co2-Sum 
  row: "Asia", Asia-Co2-Sum
  row: "North America", NA-Co2-Sum
  row: "South-America", SA-Co2-Sum
  row: "Oceania", Oceania-Co2-Sum
  row: "Europe", Europe-Co2-Sum
end

CO2-Emissions-Sum-BarChart = bar-chart(CO2-Emissions-Sum-Table, "region", "sum")
#According to the bar-chart, Asia has had the highest cumulative CO2 emissions since 1960.

#part 2:Do countries with higher emissions overall have a greater increase in temperature over time?
    

fun removes-additional-values(warming-number:: String) -> String:
  doc: "omits the plus or minus within the warming-deg-c-table and the second number"
  string-replace(warming-number, string-substring(warming-number,4,10), "")
end
#all the values had the same string length
  
fun omitting-table-values(warming-degrees-table :: Table) -> Table:
  doc:"applies removing additional values"
  transform-column(warming-degrees-table, "warming-deg-c-since-1960", removes-additional-values)
end

improved-warming-deg-c-table = omitting-table-values(new-warming-deg-c-table)

fun sanitize-degrees(num-string :: String) -> Number:
  doc:"turns temperature column into number"
  num = string-to-number(num-string)
  if 
    (is-some(num)):
    num.or-else("Not a Number")
  else:
    raise("ERROR: Cannot be translated into a number")
  end
where:
  sanitize-degrees("10") is 10
  sanitize-degrees("3.02") is 3.02
end

final-warming-deg-c-table = transform-column(improved-warming-deg-c-table, "warming-deg-c-since-1960", sanitize-degrees)





#part 2:Do countries with higher emissions overall have a greater increase in temperature over time?(reminder)


#goal: get the sum of every country and the warming degrees in the same table. remove duplicates of the country



#part 3
    

fun find-sum-country-helper(table-w-c :: Table, country :: String) -> Number:
  doc:"function that finds the sum of co2 values of a country indicated"
  
  
    fun fil(r :: Row) -> Boolean:
      r["country"] == country
    end
    # the problemis that it takes the one c-val for the one country given and then applies it to every country 
    cnval = filter-with(table-w-c, fil)
  sum(cnval, "total-co2-emission-metric-ton")
end
    
 
 

fun helper-con-to-reg(country-name :: String ) -> String:
  doc:"returns the region  of a country"
 
  asia-countries = Asia-co2.get-column("country")
  africa-countries = Africa-co2.get-column("country")
  s-america-countries = S-America-co2.get-column("country")
  n-america-countries = N-America-co2.get-column("country")
  oceania-countries = Oceania-co2.get-column("country")
  europe-countries = Europe-co2.get-column("country")
  other-countries = Other-co2.get-column("country")

  if asia-countries.member(country-name):
    "Asia"
  else if africa-countries.member(country-name):
        "Africa"
  else if s-america-countries.member(country-name):
    "South America"
  else if n-america-countries.member(country-name):
    "North America"
  else if oceania-countries.member(country-name):
    "Oceania"
  else if europe-countries.member(country-name):
    "Europe"
  else:
    "Other"
   end
end





fun makes-countrytable(co2-like :: Table) -> Table:
    doc:"Create a table w countries with high co2 and their respective co2 sums + add a col for what region they belong to" 
 
  fun build(r1 :: Row) -> Boolean:
    country = r1["country"]
     c-sum= find-sum-country-helper(co2-like, country)
    if c-sum > 2000000 :
      true
      
    else: false
    end
end

  fun build2(r:: Row):
     country = r["country"]
    find-sum-country-helper(co2-like, country)
  end
  
  
    
with-not-high = build-column(final-warming-deg-c-table,"high-co2s",build)
  build-column(with-not-high, "CO2-Values", build2)
  end
    
  
  



fun countries-whigh-co2(r :: Row) -> Boolean:
  r["high-co2s"]
end

chart-for-3 = freq-bar-chart(filter-with(makes-countrytable(new-co2-emissions),countries-whigh-co2) , "region")


CO2-Emissions-WarmingDegC-Plot = lr-plot(makes-countrytable(new-co2-emissions), "warming-deg-c-since-1960", "CO2-Values")


  fun part2number1-helper(table1 :: Table, year :: Number): #-> Number 
  doc: "find the sum of a region given a year and table containing region column"
  fun yr-values(r :: Row):
    r["year"] == year
  end
  table2 = filter-with(table1, yr-values)
  sum(table2,"total-co2-emission-metric-ton")
  
end
  


fun num1-2(years-t :: Table) :
  doc:"Takes in a table of year values and tells if Asia had the highest co2 values of that yr"

  
  fun gothrough-yrs(r :: Row):# -> Boolean:
    doc: "Builder function for col that tells if Asia had the greatest year"
    year = r["year"]
    asia-co2 = part2number1-helper(Asia-co2, year)
    sam-co2 = part2number1-helper(S-America-co2, year)
    nam-co2 = part2number1-helper(N-America-co2, year)
    africa-co2 = part2number1-helper(Africa-co2, year)
    europe-co2 = part2number1-helper(Europe-co2, year)
    oceania-co2 = part2number1-helper(Oceania-co2, year)
    other-co2 = part2number1-helper(Other-co2, year)
     if  ((asia-co2 > sam-co2) and
        (asia-co2 > nam-co2) and
        (asia-co2 > africa-co2) and
        (asia-co2 > europe-co2 ) and
        (asia-co2 > oceania-co2) and
        (asia-co2 > other-co2)):
      true
    else: false
      
    end
    
    
  end
 build-column(years-t,"Asia is top",gothrough-yrs)

end

num1-2-table = num1-2(years-1960-2014-table)
num1-2-chart = freq-bar-chart(num1-2-table, "Asia is top")


fun find-wsum-region-helper(table-w-warm :: Table, region :: String) -> Number:
  doc:"function that finds the sum of warm values of a region indicated"
  
  
    fun fil(r :: Row) -> Boolean:
    r["region"] == region
    end
   
  regnval = filter-with(table-w-warm, fil)
  mean(regnval,"warming-deg-c-since-1960")
end

fun find-sum-region(table-w-cr :: Table, region :: String) -> Number:
  doc:"function that finds the sum of warm values of a region indicated"
  
  
    fun fil(r :: Row) -> Boolean:
    r["region"] == region
    end
   
  regnval = filter-with(table-w-cr, fil)
  sum(regnval,"CO2-Values")
end

fun find-mean-region(table-w-cr :: Table, region :: String) -> Number:
  doc:"function that finds the sum of warm values of a region indicated"
  
  
    fun fil(r :: Row) -> Boolean:
    r["region"] == region
    end
   
  regnval = filter-with(table-w-cr, fil)
  mean(regnval,"CO2-Values")
end


fun summary-table(region-sums :: Table, summary-func :: (Table , String -> Number)) -> Table:
  doc: ```Produces a table that uses the given function to summarize CO2 
       emissions from 1960 to 2014 for every region (Oceania/Asia/Europe/Africa/
       SouthAmerica/NorthAmerica/Other). The outputted table should also have the 
       average warming in every region.```
  
  #regions-co2 = region-sums.get-column("sum")
  # table-w-co2 = add-col(region-sums, "CO2-summary", regions-co2)
  
  fun build-warm-avg(r :: Row)-> Number:
    region = r["region"]
    find-wsum-region-helper(final-warming-deg-c-table, region)
    end
  tww = build-column(region-sums,"avg-warming", build-warm-avg)
  
  fun build-summary(r :: Row) -> Number:
  
  summary-func(makes-countrytable(new-co2-emissions),  r["region"])
  end
  
  almost = build-column(tww ,"CO2-summary", build-summary)
  
  select-columns(almost, [list: "region", "avg-warming", "CO2-summary" ])
  
end

sum-table = summary-table(CO2-Emissions-Sum-Table,find-sum-region)
mean-table = summary-table(CO2-Emissions-Sum-Table,find-mean-region)




