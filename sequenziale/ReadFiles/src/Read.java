import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;


public class Read {

	public static void main(String[] args) {
		
		String a1="@Lettere",a2="@Economia",a3="@Fisica",a4="@Ingegneria",a5="@Matematica",a6="@Medicina",a7="@Arte",a8="@Lingue",a9="@Biologia";
		int aaaa1=0,aaaa2=0,aaaa3=0,aaaa4=0,aaaa5=0,aaaa6=0,aaaa7=0,aaaa8=0,aaaa9=0;

		for(int i=0;i<9;i++){
			try (BufferedReader br = new BufferedReader(new FileReader("centroids"+i+".txt")))
			{
				
				int aaa1=0,aaa2=0,aaa3=0,aaa4=0,aaa5=0,aaa6=0,aaa7=0,aaa8=0,aaa9=0;
				
				String sCurrentLine;
				System.out.println("Read cluster"+i);
	 
				while ((sCurrentLine = br.readLine()) != null) {
					
					
					
					if(sCurrentLine.contains(a1)){
						aaa1++;
						aaaa1++;
					} else if(sCurrentLine.contains(a2)){
						aaa2++;
						aaaa2++;
					} else if(sCurrentLine.contains(a3)){
						aaa3++;
						aaaa3++;
					} else if(sCurrentLine.contains(a4)){
						aaa4++;
						aaaa4++;
					} else if(sCurrentLine.contains(a5)){
						aaa5++;
						aaaa5++;
					} else if(sCurrentLine.contains(a6)){
						aaa6++;
						aaaa6++;
					} else if(sCurrentLine.contains(a7)){
						aaa7++;
						aaaa7++;
					} else if(sCurrentLine.contains(a8)){
						aaa8++;
						aaaa8++;
					} else if(sCurrentLine.contains(a9)){
						aaa9++;
						aaaa9++;
					}
					

				}
				int tot = aaa1+aaa2+aaa3+aaa4+aaa5+aaa6+aaa7+aaa8+aaa9;
				System.out.println("Total documents: "+tot);
				System.out.println("value <- c("+aaa1+","+aaa2+","+aaa3+","+aaa4+","+aaa5+","+aaa6+","+aaa7+","+aaa8+","+aaa9+")");
				
				
			} catch (IOException e) {
				e.printStackTrace();
			} 
			
		}
		int tot = aaaa1+aaaa2+aaaa3+aaaa4+aaaa5+aaaa6+aaaa7+aaaa8+aaaa9;
		System.out.println("Total documents: "+tot);
		System.out.println("value <- c("+aaaa1+","+aaaa2+","+aaaa3+","+aaaa4+","+aaaa5+","+aaaa6+","+aaaa7+","+aaaa8+","+aaaa9+")");
		
	}

}
