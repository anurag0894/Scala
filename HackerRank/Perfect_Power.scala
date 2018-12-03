import scala.util.control._
var number=20178373
var counter=100.0
var loop=new Breaks
loop breakable{
while(counter>=1){
var temp=scala.math.pow(number.toDouble,1/counter.toDouble)
println(temp,counter)
if(scala.math.pow(temp.toFloat,counter.toFloat)==number.toFloat){
println(counter.toFloat);
loop.break;
}
counter-=0.01;}} //bruteforcing at 0.01 interval

