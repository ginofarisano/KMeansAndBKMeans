import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;


public class Read {

	public static void main(String[] args) {
		
		String a1="Document of type Lettere",a2="Document of type Economia",a3="Document of type Fisica",a4="Document of type Ingegneria",a5="Document of type Matematica",a6="Document of type Medicina",a7="Document of type Arte",a8="Document of type Lingue",a9="Document of type Biologia";
		
		
		int i=0;
		
			try (BufferedReader br = new BufferedReader(new FileReader("output.txt")))
			{
				
				//for cluster
				int aaa1=0,aaa2=0,aaa3=0,aaa4=0,aaa5=0,aaa6=0,aaa7=0,aaa8=0,aaa9=0;
				
				String sCurrentLine;
				
				String[] split;
				
				StringBuilder all = new StringBuilder();
				
				all.append(""
						+ "printPlot8<- function(){\n"
						+ "color <- c('red','blue','yellow','violet','orange','green','pink','cyan','purple')\n"
						+ "par(mfcol=c(3,3))\n"
						+"label <- c('Lettere','Economia','Fisica','Ingegneria','Matematica','Medicina','Arte','Lingue','Biologia')\n################\n"
						);
				
				int count=1;
				
				while ((sCurrentLine = br.readLine()) != null) {
					
					
					if(sCurrentLine.contains("The cluster contains") && i>3){
						//value <- c(19,7,74,706,279,2,14,12)
						all.append("value <- c("+aaa1+","+aaa2+","+aaa3+","+aaa4+","+aaa5+","+aaa6+","+aaa7+","+aaa8+","+aaa9+")\nmymax<-max(value)\nbr<-barplot(value,ylim=c(0,max(value)),ylab='cluster"+count+"',col=color)\ntext(x=br,value+(mymax/20),labels=paste(value),xpd=NA)\ntext(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)\n\n############\n");
						
						aaa1=0;aaa2=0;aaa3=0;aaa4=0;aaa5=0;aaa6=0;aaa7=0;aaa8=0;aaa9=0;
						count++;
					}
					
					
					
					if(sCurrentLine.contains(a1)){
						split=sCurrentLine.split(" ");
						aaa1=Integer.parseInt(split[5]);
					} else if(sCurrentLine.contains(a2)){
						split=sCurrentLine.split(" ");
						aaa2=Integer.parseInt(split[5]);
						
					} else if(sCurrentLine.contains(a3)){
						split=sCurrentLine.split(" ");
						aaa3=Integer.parseInt(split[5]);
						
					} else if(sCurrentLine.contains(a4)){
						split=sCurrentLine.split(" ");
						aaa4=Integer.parseInt(split[5]);
						
					} else if(sCurrentLine.contains(a5)){
						split=sCurrentLine.split(" ");
						aaa5=Integer.parseInt(split[5]);
						
					} else if(sCurrentLine.contains(a6)){
						split=sCurrentLine.split(" ");
						aaa6=Integer.parseInt(split[5]);
						
					} else if(sCurrentLine.contains(a7)){
						split=sCurrentLine.split(" ");
						aaa7=Integer.parseInt(split[5]);
						
					} else if(sCurrentLine.contains(a8)){
						split=sCurrentLine.split(" ");
						aaa8=Integer.parseInt(split[5]);
						
					} else if(sCurrentLine.contains(a9)){
						split=sCurrentLine.split(" ");
						aaa9=Integer.parseInt(split[5]);
						
					}
					
					i++;
				}
				
				all.append("value <- c("+aaa1+","+aaa2+","+aaa3+","+aaa4+","+aaa5+","+aaa6+","+aaa7+","+aaa8+","+aaa9+")\nmymax<-max(value)\nbr<-barplot(value,ylim=c(0,max(value)),ylab='cluster"+count+"',col=color)\ntext(x=br,value+(mymax/20),labels=paste(value),xpd=NA)\ntext(x=br,-(mymax/3),labels=label,srt=90,xpd=NA)\n\n############ \n}");
				
				System.out.println(all);
			
				
			} catch (IOException e) {
				e.printStackTrace();
			} 
		
	}

}
