
def factorial(num:Int):BigInt={
var result:BigInt=1;
var i=0;
for( i <- 1 to num ){
result*=i
}
result
}

//Binomial coefficient

import scala.collection.mutable.ArrayBuffer

def bc(num:Int):ArrayBuffer[BigInt]={

var i=1;
var j=1;
var row = ArrayBuffer[BigInt]();
row+=1;
for(i <- num-1 until num){
for(j <- 1 until i+1){
row+=factorial(i)/(factorial(j)*factorial(i-j))
}
}
row
}

//row--starting row in pascal's triangle
//length--number of numbers on the stick

def pascal_main(row:Int,length:Int)={
var temp=row+length-1
var a=1
for(a <- row to temp-1){
println(bc(a)(row-1))
//println(row,temp-1)
if(a==temp-1){
println(bc(a)(row-1)+bc(a)(row))
}
}
}