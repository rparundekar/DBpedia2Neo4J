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
	private final Map<String, Set<Object>> values;
	private final Map<String, double[]> bins;
	private final Set<String> shouldNotBin;
	public Binner(){
		this.values=new HashMap<>();
		this.bins=new HashMap<>();
		this.shouldNotBin=new HashSet<>();
	}

	public void bin(String attribute, Object value) {
		if(value instanceof String){
			try{
				value=Double.parseDouble(value.toString());
			}catch(Exception e){
			}
		}
		Set<Object> vs=values.get(attribute);
		if(vs==null){
			vs=new HashSet<>();
			values.put(attribute,vs);
		}
		if(!vs.contains(value))
			vs.add(value);
	}

	public void buildBins(){
		for(String attribute:values.keySet()){
			Set<Object> vs=values.get(attribute);
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
				shouldNotBin.add(attribute);
			}
		}
		//Clear the values to free some space;
		values.clear();
		System.gc();
	}

	public String getBin(String attribute, Object value){
		if(!this.shouldNotBin.contains(attribute)){
			if(value instanceof String){
				try{
					value=Double.parseDouble(value.toString());
				}catch(Exception e){
				}
			}
			if(value instanceof Number){
				if(bins.containsKey(attribute)){
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
				}
			}else{
				return "outlier_s";
			}
		}
		return value.toString().trim();
	}

	public void writeToFile(File parentFile) {
		File folder=new File(parentFile, "attributes");
		if(!folder.exists())
			folder.mkdir();

		try {
			PrintWriter binsPrintWriter=new PrintWriter(new File(folder,"bins.csv"));
			binsPrintWriter.println("attribute, bins");
			for(String attribute:values.keySet()){
				PrintWriter pw=new PrintWriter(new File(folder,attribute+".csv"));
				pw.println(attribute + ", bin");
				Set<Object> vs=values.get(attribute);
				for(Object o:vs){
					pw.println(o.toString() +"," + getBin(attribute, o));
					pw.flush();
				}

				if(bins.containsKey(attribute)){
					String str=Arrays.toString(bins.get(attribute));
					binsPrintWriter.println(attribute + "," + str.substring(1, str.length()-1).trim());
					binsPrintWriter.flush();
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
}
