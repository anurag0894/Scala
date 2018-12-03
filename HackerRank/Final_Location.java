import java.util.*;

//If queries     source--Ban
//             Destination--Mum

//Output

//  Final Source--Del(Del->Kol,Kol->Ban implies Del is the originating source)
//  Final Destination--Che(Koc->Mum,Mum->Che implies Che is the final destination)
  
public class Java_test {  
	
	private static String getKey(String value,Map<String, String> map){
	    for(String key : map.keySet()){
	        if(map.get(key).equals(value)){
	            return key; //return the first found
	        }
	    }
	    return value;
	}
	   
	
public static void main(String[] args) {  
    Map map=new HashMap();  
    //Adding elements to map  
    map.put("Kol","Ban");  
    map.put("Del","Kol");  
    map.put("Ban","Ran");  
    map.put("Koc","Mum"); 
    map.put("Mum","Che");
	String[] Keys=new String[map.size()];
	String[] Values=new String[map.size()];
	
    //Traversing Map  
    Scanner in=new Scanner(System.in);
    String source=in.nextLine();
    String destination=in.nextLine();
    Set set=map.entrySet();//Converting to Set so that we can traverse  
    Iterator itr=set.iterator(); 
    String mid_variable="";
    //Finding Final Source
    int i=0;
    while(itr.hasNext()){  
        Map.Entry entry=(Map.Entry)itr.next();  
        Keys[i]=entry.getKey().toString();
        Values[i]=entry.getValue().toString();
        i++;
        }
    
  // System.out.println(Arrays.asList(Values)); 

    int a=0;
    while(a==0) {
    	if(Arrays.asList(Values).contains(source)) {
    		source=getKey(source,map);
    	}
    	else {
    		a=1;
    	}
    }
    
    a=0;
    while(a==0) {
    	if(Arrays.asList(Keys).contains(destination)) {
    		destination=map.get(destination).toString();
    	}
    	else {
    		a=1;
    	}
    }
    
    
    String source_status= (Arrays.asList(Keys).contains(source) && source != null ) ? source:"No Source Found";
    String destination_status=(Arrays.asList(Values).contains(destination) && destination!=null) ? destination: "No Destination Found"  ;
    System.out.format("final_source:= %s \nfinal_destination:=%s" , source_status,destination_status ) ;
    
  in.close();

       
        }

    
 
    }  

  