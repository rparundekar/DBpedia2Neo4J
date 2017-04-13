package randomWalks;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Binner {
	// SLF4J Logger bound to Log4J 
	private static final Logger logger=LoggerFactory.getLogger(Binner.class);
	private Map<String, Map<Object, Integer>> values;
	private final Map<String, double[]> bins;
	private final Set<String> binByValue;
	public Binner(){
		this.values=new HashMap<>();
		this.bins=new HashMap<>();
		this.binByValue=new HashSet<>();
	}

	public void bin(String attribute, Object value) {
		if(value instanceof String){
			try{
				value=Double.parseDouble(value.toString());
			}catch(Exception e){
			}
		}
		Map<Object, Integer> vs=values.get(attribute);
		if(vs==null){
			vs=new HashMap<>();
			values.put(attribute,vs);
		}
		if(!vs.containsKey(value))
			vs.put(value,1);
		else
			vs.put(value, vs.get(value)+1);
	}

	public void buildBins(){
		for(String attribute:values.keySet()){
			Set<Object> vs=values.get(attribute).keySet();
			List<Double> collection=new ArrayList<>();
			for(Object o:vs){
				if(o instanceof Number){
					collection.add(((Number)o).doubleValue());
				}
			}
			//Atleast 95% of them should be numbers
			if((collection.size()*1.0/vs.size())>0.95){
				Collections.sort(collection);
				double q1=collection.get(collection.size()/4);
				double q3=collection.get(collection.size()*3/4);
				double iqr=q3-q1;
				double low = q1-(1.5*iqr);
				double high = q3+(1.5*iqr);
				double binWidth = (2 * iqr / Math.pow(collection.size(), 1.0/3))/3.0; // Bin width by Freedman–Diaconis' choice/3
				int numberOfBins = (int)Math.ceil((high-low)/binWidth);
				if(numberOfBins<=0)
					numberOfBins=1;

				double[] bin=new double[numberOfBins];
				bin[0]=low;
				for(int i=1;i<bin.length;i++){
					bin[i]=bin[i-1]+binWidth;
				}
				bins.put(attribute, bin);
			}else{
				Map<Object, Integer> valueCounts =  values.get(attribute);
				int uniqueCount=0;
				int countOfValues=0;
				for(Object o: valueCounts.keySet()){
					int count=valueCounts.get(o);
					countOfValues+=count;
					uniqueCount++;
				}
				double load = uniqueCount*1.0/countOfValues;
				if(load<0.9){
					binByValue.add(attribute);
				}else{
					//doNotBin
				}
			}
		}
	}

	public String getBin(String attribute, Object value){
		if(this.bins.containsKey(attribute)){
			if(value instanceof String){
				try{
					value=Double.parseDouble(value.toString());
				}catch(Exception e){
				}
			}
			if(value instanceof Number){
				double[] bin=null;
				bin=bins.get(attribute);
				double n=((Number)value).doubleValue();
				if(n<bin[0])
					return "outlier_l";
				if(n>=bin[bin.length-1])
					return "outlier_u";
				for(int i=0;i<bin.length-1;i++){
					if(n>=bin[i] && n<bin[i+1])
						return "bin_"+i;
				}
				return value.toString();
			}else{
				return "outlier_s";
			}
		}
		else if(this.binByValue.contains(attribute)){
			return value.toString().trim().replaceAll("[^A-Za-z0-9]", "_");
		}else{
			return null;
		}
	}

	public void writeToFile(File parentFile) {
		File folder=new File(parentFile, "attributes");
		if(!folder.exists())
			folder.mkdir();
		try {
			PrintWriter binsPrintWriter=new PrintWriter(new File(folder,"bins.csv"));
			binsPrintWriter.println("attribute, bins");
			for(String attribute:values.keySet()){
				if(this.bins.containsKey(attribute) || this.binByValue.contains(attribute)){
					PrintWriter pw=new PrintWriter(new File(folder,attribute+".csv"));
					pw.println(attribute + ", bin");
					Set<Object> vs=values.get(attribute).keySet();
					for(Object o:vs){
						String bin = getBin(attribute, o);
						if(bin==null)
							bin="";
						pw.println(o.toString() +"," + bin);
						pw.flush();
					}

					if(bins.containsKey(attribute)){
						String str=Arrays.toString(bins.get(attribute));
						binsPrintWriter.println(attribute + "," + str.substring(1, str.length()-1).trim());
						binsPrintWriter.flush();
					}
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		//Clear the values to free some space;
		logger.info("Cleaning up some memory");
		values=null;
		System.gc();
	}
}
