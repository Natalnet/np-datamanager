package com.np.datamanager.service;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.np.commons.utils.Utils;

@Service
public class DataManagerServiceImpl implements DataManagerService 
{
	@Value("${storage.path.base}")
	private String dataFileBaseDir;
	
	/**
	 * upload a data file to store it locally
	 * 
	 * @param file: a data file to store
	 * @param urlRepo: a http address
	 * @param localeCode: a locale code
	 * @param fileName: a name to file
	 * 
	 */
	@Override
	public void uploadRawDataAndStoreLocally(MultipartFile file, String urlRepoKey, String path, String fileName) throws Exception
	{
		final Path localePath = Paths.get(dataFileBaseDir.concat("/").concat(urlRepoKey).concat("-dir/").concat(path.replace(":", "/")));
		
		try
		{
			localePath.toFile().mkdirs();
			
			BufferedWriter writer = new BufferedWriter(new FileWriter(localePath.toFile().getPath().concat("/").concat(fileName.concat(".feature"))));
			writer.write(IOUtils.toString(file.getInputStream(), StandardCharsets.UTF_8.name()));
			writer.close();
		}
		catch (IOException e)
		{
			throw new Exception("error: error trying to download file from repository [".concat(e.getMessage()).concat("]"));
		}
	}
	
	/**
	 * download a raw data file from a url and store it locally
	 * 
	 * @param urlRepo: a http address
	 * 
	 */
	@Override
	public void downloadRawDataAndStoreLocally(String urlRepo) throws Exception 
	{
		try (BufferedInputStream bufferedInputStream = new BufferedInputStream(new URL(urlRepo).openStream())) 
		{
			final String dirPath = dataFileBaseDir.concat("/").concat(Utils.getInstance().hashCode(urlRepo)).concat("-dir");
			final File dir = new File(dirPath);
			if (!dir.exists())
			{
				dir.mkdirs();
			}
			
			final String repositoryFilePath = dataFileBaseDir.concat("/").concat(Utils.getInstance().hashCode(urlRepo)).concat(".repo");
			try (FileOutputStream fileOutputStream = new FileOutputStream(repositoryFilePath))
			{
				byte dataBuffer[] = new byte[1024];
				int bytesRead;
				
				fileOutputStream.write(urlRepo.concat("\n").getBytes());
				
				while ((bytesRead = bufferedInputStream.read(dataBuffer, 0, 1024)) != -1) 
				{
					fileOutputStream.write(dataBuffer, 0, bytesRead);
				}
			} 
			catch (IOException e) 
			{
				throw new Exception("error: error trying to download file from repository [".concat(e.getMessage()).concat("]"));
			}
		} 
		catch (IOException e) 
		{
			throw new Exception("error: error trying to download file from repository [".concat(e.getMessage()).concat("]"));
		}
	}
	
	/**
	 * download a data file from a url and store it locally
	 * 
	 * @param urlRepo: a http address
	 * @param path: a locale path
	 * @param fileName: a name for a file (ends with .feature)
	 * 
	 */
	/*
	@Override
	public void downloadDataAndStoreLocally(String urlRepo, String path, String fileName) throws Exception 
	{
		try (BufferedInputStream bufferedInputStream = new BufferedInputStream(new URL(urlRepo).openStream())) 
		{
			String dirFeatures = dataFileBaseDir.concat("/").concat(Utils.getInstance().hashCode(urlRepo)).concat("-dir/").concat(path).concat("/");
			final File dir = new File(dirFeatures);
			if (!dir.exists())
			{
				dir.mkdirs();
			}
			
			fileName = fileName.concat(".feature");
			String featuresFilePath = dataFileBaseDir.concat("/").concat(Utils.getInstance().hashCode(urlRepo)).concat("-dir/").concat(path).concat("/").concat(fileName);
			try (FileOutputStream fileOutputStream = new FileOutputStream(featuresFilePath))
			{
				byte dataBuffer[] = new byte[1024];
				int bytesRead;
				
				fileOutputStream.write(urlRepo.concat(" - ").concat(fileName).concat("\n").getBytes());
				
				while ((bytesRead = bufferedInputStream.read(dataBuffer, 0, 1024)) != -1) 
				{
					fileOutputStream.write(dataBuffer, 0, bytesRead);
				}
			} 
			catch (IOException e) 
			{
				throw new Exception("error: error trying to download file from repository [".concat(e.getMessage()).concat("]"));
			}
		} 
		catch (IOException e) 
		{
			throw new Exception("error: error trying to download file from repository [".concat(e.getMessage()).concat("]"));
		}
	}
	 */
	
	@Override
	public JSONArray listAllRawDatas() throws Exception 
	{
		ProcessBuilder processBuilder = null;
		Process process = null;
		BufferedReader cReader = null;
		BufferedReader fReader = null;
		
		try 
		{
			final JSONArray jsArray = new JSONArray();
			
			processBuilder = new ProcessBuilder();
			processBuilder.command("bash", "-c", "ls ".concat(dataFileBaseDir.concat("/")));
			process = processBuilder.start();
			cReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
			
			String cLine = null;
			while ((cLine = cReader.readLine()) != null) 
			{
				if (cLine.endsWith(".repo"))
				{
					final String fileName = dataFileBaseDir.concat("/").concat(cLine);
					fReader = new BufferedReader(new FileReader(fileName));
					String fLine = null;
					if ((fLine = fReader.readLine()) != null) 
					{
						JSONObject jsObject = new JSONObject();
						jsObject.put("urlRepo", fLine);
						jsObject.put("urlRepoKey", Utils.getInstance().hashCode(fLine));
						jsArray.put(jsObject);
					}
				}
			}
			
			return jsArray;
		} 
		catch (IOException e) 
		{
			throw new Exception("error: internal system error [".concat(e.getMessage()).concat("]"));
		}
		finally 
		{
			if (fReader != null) 
			{
				fReader.close();
			}
			
			if (cReader != null)  
			{
				cReader.close();
			}
		}
	}
	
	/**
	 * list all files *.feature order by path for a repository.
	 * 
	 * example: 
	 * brazil:rn:natal, all; /brazil/rn/natal/death.feature
	 * response:
	 * {
	 *    "brazil": {
	 *       "rn": {
	 *          "natal": {
	 *             "files": ["all.feature","death.feature"]
	 *          },
	 *          "files": ["all.feature"]
	 *       }
	 *    }
	 * }
	 * 
	 * @param urlRepo: a http address
	 * 
	 */
	@Override
	public JSONObject listDatasByRepo(String urlRepoKey) throws Exception 
	{
		try 
		{
			final String featuresPath = dataFileBaseDir.concat("/").concat(urlRepoKey).concat("-dir/");
			
			JSONObject jdDirSchema = new JSONObject();
			
			getDirSchema(featuresPath, jdDirSchema);
			
			return jdDirSchema;
		} 
		catch (IOException e) 
		{
			throw new Exception("error: internal system error [".concat(e.getMessage()).concat("]"));
		}
	}
	
	/**
	 * get a data file
	 * 
	 * @param urlRepo: a http address
	 * @param path: a locale path
	 * @param fileName: a name for a file (ends with .feature)
	 * 
	 */
	@Override
	public Resource getData(String urlRepoKey, String path, String fileName) throws Exception
	{
		try 
		{
			final String featureFilePath = dataFileBaseDir.concat("/").concat(urlRepoKey).concat("-dir/").concat(path).concat("/").concat(fileName);
			final Resource resource = new UrlResource(Paths.get(featureFilePath).toUri());
			if (resource.exists())
			{
				return resource;
			}
			else
			{
				return null;
			}
		} 
		catch (MalformedURLException e) 
		{
			throw new Exception("error: internal system error [".concat(e.getMessage()).concat("]"));
		}
	}
	
	/**
	 * delete a data file
	 * 
	 * @param urlRepo: a http address
	 * @param path: a locale path
	 * @param fileName: a name for a file (ends with .feature)
	 * 
	 */
	@Override
	public void removeData(String urlRepoKey, String path, String fileName) throws Exception 
	{
		try {
			ProcessBuilder processBuilder = new ProcessBuilder();
			final String featureFilePath = dataFileBaseDir.concat("/").concat(urlRepoKey).concat("-dir/").concat(path).concat("/").concat(fileName);
			processBuilder.command("bash", "-c", "rm -f ".concat(dataFileBaseDir).concat(featureFilePath));
			Process process = processBuilder.start();
			
			int exitVal = process.waitFor();
			process.destroy();
			if (exitVal != 0) 
			{
				throw new Exception("error: can't remove files. code error: ".concat(String.valueOf(exitVal)));
			}
		}
		catch (InterruptedException | IOException e) 
		{
			throw new Exception("error: internal system error [".concat(e.getMessage()).concat("]"));
		}
	}
	
	private void getDirSchema(String repoBaseDir, JSONObject dirSchema) throws Exception
	{
		FileUtils.listFiles(new File(repoBaseDir), new RegexFileFilter("^(.*?)"), DirectoryFileFilter.DIRECTORY).forEach(file->
		{
			if (file.getPath().endsWith(".feature"))
			{
				String [] localePathTokens = file.getPath().replace(repoBaseDir, "").split(Pattern.quote("/"));
				
				switch (localePathTokens.length) 
				{
					case 2:
						if (dirSchema.has(localePathTokens[0]))
						{
							dirSchema.getJSONObject(localePathTokens[0]).getJSONArray("files").put(localePathTokens[1]);
						}
						else
						{
							dirSchema.put(localePathTokens[0], new JSONObject());
							dirSchema.getJSONObject(localePathTokens[0]).put("files", new JSONArray());
							dirSchema.getJSONObject(localePathTokens[0]).getJSONArray("files").put(localePathTokens[1]);
						}
						break;
					
					case 3:
						if (dirSchema.has(localePathTokens[0]))
						{
							if (dirSchema.getJSONObject(localePathTokens[0]).has(localePathTokens[1]))
							{
								dirSchema.getJSONObject(localePathTokens[0]).getJSONObject(localePathTokens[1]).getJSONArray("files").put(localePathTokens[2]);
							}
							else
							{
								dirSchema.getJSONObject(localePathTokens[0]).put(localePathTokens[1], new JSONObject());
								dirSchema.getJSONObject(localePathTokens[0]).getJSONObject(localePathTokens[1]).put("files", new JSONArray());
								dirSchema.getJSONObject(localePathTokens[0]).getJSONObject(localePathTokens[1]).getJSONArray("files").put(localePathTokens[2]);
							}
						}
						else
						{
							dirSchema.put(localePathTokens[0], new JSONObject());
							dirSchema.getJSONObject(localePathTokens[0]).put(localePathTokens[1], new JSONObject());
							dirSchema.getJSONObject(localePathTokens[0]).getJSONObject(localePathTokens[1]).put("files", new JSONArray());
							dirSchema.getJSONObject(localePathTokens[0]).getJSONObject(localePathTokens[1]).getJSONArray("files").put(localePathTokens[2]);
						}
						break;
						
					case 4:
						if (dirSchema.has(localePathTokens[0]))
						{
							if (dirSchema.getJSONObject(localePathTokens[0]).has(localePathTokens[1]))
							{
								if (dirSchema.getJSONObject(localePathTokens[0]).getJSONObject(localePathTokens[1]).has(localePathTokens[2]))
								{
									dirSchema.getJSONObject(localePathTokens[0]).getJSONObject(localePathTokens[1]).getJSONObject(localePathTokens[2]).getJSONArray("files").put(localePathTokens[3]);
								}
								else
								{
									dirSchema.getJSONObject(localePathTokens[0]).getJSONObject(localePathTokens[1]).put(localePathTokens[2], new JSONArray());
									dirSchema.getJSONObject(localePathTokens[0]).getJSONObject(localePathTokens[1]).getJSONObject(localePathTokens[2]).put("files", new JSONArray());
									dirSchema.getJSONObject(localePathTokens[0]).getJSONObject(localePathTokens[1]).getJSONObject(localePathTokens[2]).getJSONArray("files").put(localePathTokens[3]);
								}
							}
							else
							{
								dirSchema.getJSONObject(localePathTokens[0]).put(localePathTokens[1], new JSONObject());
								dirSchema.getJSONObject(localePathTokens[0]).getJSONObject(localePathTokens[1]).put(localePathTokens[2], new JSONArray());
								dirSchema.getJSONObject(localePathTokens[0]).getJSONObject(localePathTokens[1]).getJSONObject(localePathTokens[2]).put("files", new JSONArray());
								dirSchema.getJSONObject(localePathTokens[0]).getJSONObject(localePathTokens[1]).getJSONObject(localePathTokens[2]).getJSONArray("files").put(localePathTokens[3]);
							}
						}
						else
						{
							dirSchema.put(localePathTokens[0], new JSONObject());
							dirSchema.getJSONObject(localePathTokens[0]).put(localePathTokens[1], new JSONObject());
							dirSchema.getJSONObject(localePathTokens[0]).getJSONObject(localePathTokens[1]).put(localePathTokens[2], new JSONObject());
							dirSchema.getJSONObject(localePathTokens[0]).getJSONObject(localePathTokens[1]).getJSONObject(localePathTokens[2]).put("files", new JSONArray());
							dirSchema.getJSONObject(localePathTokens[0]).getJSONObject(localePathTokens[1]).getJSONObject(localePathTokens[2]).getJSONArray("files").put(localePathTokens[3]);
						}
						break;
						
					default:
						throw new RuntimeException("error: path hierarchy unknow");
				}
			}
		});
	}
	
	/**
	 * get a body data file as JSON Object for a period.
	 * 
	 * @param urlRepoKey: a http address
	 * @param path: a locale path
	 * @param feature: a feature name
	 * 
	 */
	@Override
	public JSONArray getDataAsJSON(String urlRepoKey, String path, String [] features) throws Exception 
	{
		FileReader fReader = null;
		BufferedReader bReader = null;
		
		try 
		{
			fReader = new FileReader(new File(dataFileBaseDir.concat("/").concat(urlRepoKey).concat("-dir/").concat(path).concat("/all.feature")));
			bReader = new BufferedReader(fReader);
			
			JSONArray jsArray = new JSONArray();
			
			JSONObject [] headerRow = null;
			String [] rowDataTokens = null;
			String row = null;

			// read header line
			if ((row = bReader.readLine()) != null)
			{
				rowDataTokens = row.split(Pattern.quote(","));
				
				headerRow = new JSONObject[features.length];
				for (int headerRowColumnIndex = 0; headerRowColumnIndex < features.length; headerRowColumnIndex++) 
				{
					headerRow[headerRowColumnIndex] = null;
					for (int rowTokenColumnIndex = 0; rowTokenColumnIndex < rowDataTokens.length; rowTokenColumnIndex++) 
					{
						if (features[headerRowColumnIndex].equalsIgnoreCase(rowDataTokens[rowTokenColumnIndex]))
						{
							headerRow[headerRowColumnIndex] = new JSONObject();
							headerRow[headerRowColumnIndex].put("index", rowTokenColumnIndex);
							headerRow[headerRowColumnIndex].put("name", rowDataTokens[rowTokenColumnIndex]);
						}
					}
				}
			}
			else
			{
				throw new Exception("error: data file do not begin with headers");
			}
			
			// read header meaning description line
			bReader.readLine();
			
			while ((row = bReader.readLine()) != null) 
			{
				rowDataTokens = row.concat(" ").split(Pattern.quote(","));
				JSONObject jsObject = new JSONObject();
				for (int headerRowColumnIndex = 0; headerRowColumnIndex < headerRow.length; headerRowColumnIndex++) 
				{
					if (headerRow[headerRowColumnIndex] != null && rowDataTokens[headerRow[headerRowColumnIndex].getInt("index")] != null)
					{
						jsObject.put(headerRow[headerRowColumnIndex].getString("name"), rowDataTokens[headerRow[headerRowColumnIndex].getInt("index")]);
					}
				}
				jsArray.put(jsObject);
			}
			
			return jsArray;
		} 
		catch (MalformedURLException e) 
		{
			throw new Exception("error: internal system error [".concat(e.getMessage()).concat("]"));
		}
		finally 
		{
			if (bReader != null) 
			{
				bReader.close();
			}

			if (fReader != null) 
			{
				fReader.close();
			}
		}
	}
	
	/**
	 * get a body data file as JSON Object for a period.
	 * 
	 * @param urlRepoKey: a http address
	 * @param path: a locale path
	 * @param features: a list of String, each item in the list is the name of a feature
	 * @param begin: the start date of the series
	 * @param end: the end date of the series
	 * 
	 */
	@SuppressWarnings("deprecation")
	@Override
	public JSONArray getDataAsJSON(String urlRepoKey, String path, String [] features, String begin, String end) throws Exception 
	{
		FileReader fReader = null;
		BufferedReader bReader = null;
		
		try 
		{
			fReader = new FileReader(new File(dataFileBaseDir.concat("/").concat(urlRepoKey).concat("-dir/").concat(path).concat("/all.feature")));
			bReader = new BufferedReader(fReader);
			
			JSONArray jsArray = new JSONArray();
			
			JSONObject [] headerRow = null;
			String [] rowTokens = null;
			String row = null;
			
			// read header line
			if ((row = bReader.readLine()) != null)
			{
				rowTokens = row.split(Pattern.quote(","));
				
				headerRow = new JSONObject[features.length];
				for (int headerRowColumnIndex = 0; headerRowColumnIndex < headerRow.length; headerRowColumnIndex++) 
				{
					headerRow[headerRowColumnIndex] = null;
					for (int rowTokenColumnIndex = 0; rowTokenColumnIndex < rowTokens.length; rowTokenColumnIndex++) 
					{
						if (rowTokens[rowTokenColumnIndex].equalsIgnoreCase(features[headerRowColumnIndex]))
						{
							headerRow[headerRowColumnIndex] = new JSONObject();
							headerRow[headerRowColumnIndex].put("index", rowTokenColumnIndex);
							headerRow[headerRowColumnIndex].put("name", rowTokens[rowTokenColumnIndex]);
						}
					}
				}
			}
			else
			{
				throw new Exception("error: data file do not begin with headers");
			}
			
			// read header meaning description line
			bReader.readLine();
			
			while ((row = bReader.readLine()) != null) 
			{
				rowTokens = row.split(Pattern.quote(","));
				
				String [] tmp = begin.split(Pattern.quote("-"));
				Long beginDate = new Date(Integer.parseInt(tmp[0])-1900, Integer.parseInt(tmp[1])-1, Integer.parseInt(tmp[2])).getTime();
				
				tmp = end.split(Pattern.quote("-"));
				Long endDate = new Date(Integer.parseInt(tmp[0])-1900, Integer.parseInt(tmp[1])-1, Integer.parseInt(tmp[2])).getTime();
				
				tmp = rowTokens[1].split(Pattern.quote("-"));
				Long date = new Date(Integer.parseInt(tmp[0])-1900, Integer.parseInt(tmp[1])-1, Integer.parseInt(tmp[2])).getTime();
				if (date >= beginDate && date <= endDate)
				{
					JSONObject jsObject = new JSONObject();
					for (int headerRowColumnIndex = 0; headerRowColumnIndex < headerRow.length; headerRowColumnIndex++) 
					{
						jsObject.put(headerRow[headerRowColumnIndex].getString("name"), rowTokens[headerRow[headerRowColumnIndex].getInt("index")]);
					}
					jsArray.put(jsObject);
				}
			}
			
			return jsArray;
		} 
		catch (MalformedURLException e) 
		{
			throw new Exception("error: internal system error [".concat(e.getMessage()).concat("]"));
		}
		finally 
		{
			if (bReader != null) 
			{
				bReader.close();
			}
			
			if (fReader != null) 
			{
				fReader.close();
			}
		}
	}
	
	/**
	 * get a body data file as  CSV String.
	 * 
	 * @param urlRepoKey: a http address
	 * @param path: a locale path
	 * @param features: a list of String, each item in the list is the name of a feature
	 * 
	 * @return o retorno será uma String em formato CSV
	 * 
	 */
	@Override
	public String getDataAsCSV(String urlRepoKey, String path, String [] features) throws Exception 
	{
		FileReader fReader = null;
		BufferedReader bReader = null;
		
		try 
		{
			fReader = new FileReader(new File(dataFileBaseDir.concat("/").concat(urlRepoKey).concat("-dir/").concat(path).concat("/all.feature")));
			bReader = new BufferedReader(fReader);
			
			StringBuffer buffer = new StringBuffer();
			
			JSONObject [] headerRow = null;
			String row = null;
			String [] rowTokens = null;
			
			// read header line
			if ((row = bReader.readLine()) != null)
			{
				rowTokens = row.split(Pattern.quote(","));
				
				buffer.append("datetime,");
				
				headerRow = new JSONObject[features.length];
				for (int headerRowColumnIndex = 0; headerRowColumnIndex < headerRow.length; headerRowColumnIndex++) 
				{
					headerRow[headerRowColumnIndex] = null;
					for (int rowTokenColumnIndex = 0; rowTokenColumnIndex < rowTokens.length; rowTokenColumnIndex++) 
					{
						if (rowTokens[rowTokenColumnIndex].equalsIgnoreCase(features[headerRowColumnIndex]))
						{
							headerRow[headerRowColumnIndex] = new JSONObject();
							headerRow[headerRowColumnIndex].put("index", rowTokenColumnIndex);
							
							buffer.append(rowTokens[rowTokenColumnIndex]).append(",");
						}
					}
				}
			}
			else
			{
				throw new Exception("error: data file do not begin with headers");
			}
			
			// read header meaning description line
			bReader.readLine();
			
			while ((row = bReader.readLine()) != null) 
			{
				buffer.append(rowTokens[1]).append(",");
					
				for (int headerRowColumnIndex = 0; headerRowColumnIndex < headerRow.length; headerRowColumnIndex++) 
				{
					buffer.append(rowTokens[headerRow[headerRowColumnIndex].getInt("index")]).append(",");
				}
				
				buffer.delete(buffer.length()-1, buffer.length()).append("\n");
			}
			
			return buffer.toString();
		} 
		catch (MalformedURLException e) 
		{
			throw new Exception("error: internal system error [".concat(e.getMessage()).concat("]"));
		}
		finally 
		{
			if (bReader != null) 
			{
				bReader.close();
			}

			if (fReader != null) 
			{
				fReader.close();
			}
		}
	}
	
	/**
	 * get a body data file as  CSV String for a period.
	 * 
	 * @param urlRepoKey: a http address
	 * @param path: a locale path
	 * @param features: a list of String, each item in the list is the name of a feature
	 * @param begin: the start date of the series
	 * @param end: the end date of the series
	 * 
	 * @return o retorno será uma String em formato CSV, obedecendo a ordem explicitada no parametro features
	 * 
	 */
	@SuppressWarnings("deprecation")
	@Override
	public String getDataAsCSV(String urlRepoKey, String path, String [] features, String begin, String end) throws Exception 
	{
		FileReader fReader = null;
		BufferedReader bReader = null;
		
		try 
		{
			fReader = new FileReader(new File(dataFileBaseDir.concat("/").concat(urlRepoKey).concat("-dir/").concat(path).concat("/all.feature")));
			bReader = new BufferedReader(fReader);
			
			StringBuffer buffer = new StringBuffer();
			
			JSONObject [] headerRow = null;
			String [] rowTokens = null;
			String row = null;
			
			if ((row = bReader.readLine()) != null)
			{
				rowTokens = row.split(Pattern.quote(","));
				
				headerRow = new JSONObject[features.length];
				for (int headerRowColumIndex = 0; headerRowColumIndex < headerRow.length; headerRowColumIndex++) 
				{
					headerRow[headerRowColumIndex] = null;
					for (int rowTokenheaderRow = 0; rowTokenheaderRow < rowTokens.length; rowTokenheaderRow++) 
					{
						if (rowTokens[rowTokenheaderRow].equalsIgnoreCase(features[headerRowColumIndex]))
						{
							headerRow[headerRowColumIndex] = new JSONObject();
							headerRow[headerRowColumIndex].put("index", rowTokenheaderRow);
							headerRow[headerRowColumIndex].put("name", rowTokens[rowTokenheaderRow]);
							
							buffer.append(rowTokens[rowTokenheaderRow]).append(",");
						}
					}
				}
			}
			else
			{
				throw new Exception("error: data file do not begin with headers");
			}
			
			// read header meaning description line
			bReader.readLine();
			
			while ((row = bReader.readLine()) != null) 
			{
				rowTokens = row.split(Pattern.quote(","));
				
				String [] tmp = begin.split(Pattern.quote("-"));
				Long beginDate = new Date(Integer.parseInt(tmp[0])-1900, Integer.parseInt(tmp[1])-1, Integer.parseInt(tmp[2])).getTime();
				
				tmp = end.split(Pattern.quote("-"));
				Long endDate = new Date(Integer.parseInt(tmp[0])-1900, Integer.parseInt(tmp[1])-1, Integer.parseInt(tmp[2])).getTime();
				
				tmp = rowTokens[1].split(Pattern.quote("-"));
				Long date = new Date(Integer.parseInt(tmp[0])-1900, Integer.parseInt(tmp[1])-1, Integer.parseInt(tmp[2])).getTime();
				
				if (date >= beginDate && date <= endDate)
				{
					for (int headerRowColumnIndex = 0; headerRowColumnIndex < headerRow.length; headerRowColumnIndex++) 
					{
						buffer.append(rowTokens[headerRow[headerRowColumnIndex].getInt("index")]).append(",");
					}
				}
				
				buffer.delete(buffer.length()-1, buffer.length()).append("\n");
			}
			
			return buffer.toString();
		} 
		catch (MalformedURLException e) 
		{
			throw new Exception("error: internal system error [".concat(e.getMessage()).concat("]"));
		}
		finally 
		{
			if (bReader != null) 
			{
				bReader.close();
			}
			
			if (fReader != null) 
			{
				fReader.close();
			}
		}
	}
	
	/**
	 * get a body data file as CSV String.
	 * 
	 * @param urlRepoKey: a http address
	 * @param path: a locale path
	 * 
	 * @return o retorno será uma String em formato CSV
	 * 
	 */
	@Override
	public String getDataFeatures(String urlRepoKey, String path) throws Exception
	{
		try 
		{
			StringBuffer buffer = new StringBuffer();
			
			String filePath = dataFileBaseDir.concat("/").concat(urlRepoKey).concat("-dir/").concat(path).concat("/all.feature");
			File file = new File(filePath);
			FileReader fReader = new FileReader(file);
			BufferedReader bReader = new BufferedReader(fReader);
			
			int lineCount = 0;
			
			String line = null;
			while ((line = bReader.readLine()) != null && lineCount < 5) 
			{
				String [] fileRow = line.split(Pattern.quote(","));
				for (int fileColumn = 0; fileColumn < fileRow.length; fileColumn++) 
				{
					buffer.append(fileRow[fileColumn]).append(",");
				}
				
				buffer.delete(buffer.length()-1, buffer.length()).append("\n");
				
				lineCount++;
			}
			
			if (bReader != null) 
			{
				bReader.close();
				fReader.close();
			}
			
			return buffer.toString();
		} 
		catch (MalformedURLException e) 
		{
			throw new Exception("error: internal system error [".concat(e.getMessage()).concat("]"));
		}
	}
	
	public void slice(String repo, JSONObject info) throws Exception
	{
		/*
		{
		  "locale": "SP"
		  "columns": [],
		  "separator": ",",
		  "connective": "and",
		  "cValueIndex": [],
		  "cValue": []
		}
		 */

		InputStream inputStream = null;
		BufferedReader bufferedReader = null;

		if (!info.has("connective")) 
		{
			info.put("connective", "and");
		}

		if (!info.has("separator")) 
		{
			info.put("separator", ",");
		}

		try
		{
			int [] columns = new int [info.getJSONArray("columns").length()];

			for (int i = 0; i < info.getJSONArray("columns").length(); i++)
			{
				columns[i] = info.getJSONArray("columns").getInt(i);
			}

			final String pathToRepo = dataFileBaseDir.concat("/").concat(repo).concat(".repo");

			final StringBuffer buffer = new StringBuffer();

			try (Stream<String> lines = Files.lines(Paths.get(pathToRepo)))
			{
				final String [] token = lines.skip(1).findFirst().get().split(Pattern.quote(info.getString("separator")));

				for (int index = 0; index < columns.length; index++)
				{
					if (index > 0)
					{
						buffer.append(info.getString("separator"));
					}
					buffer.append(token[columns[index]]);
				}
				
				buffer.append("\n");
			}

			inputStream = new FileInputStream(pathToRepo);
			bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

			final JSONObject counter = new JSONObject();
			counter.put("count", 0);

			bufferedReader.lines().skip(2).map(line -> {
				final String [] token = line.concat(" ").split(info.getString("separator"));

				boolean flag;

				if (info.getString("connective").equals("or"))
				{
					flag = false;

					for (int i = 0; i < info.getJSONArray("cValueIndex").length(); i++)
					{
						if (token[info.getJSONArray("cValueIndex").getInt(i)].equalsIgnoreCase(info.getJSONArray("cValue").getString(i)))
						{
							flag = true; break;
						}
					}
				}
				else 
				{
					flag = true;

					for (int i = 0; i < info.getJSONArray("cValueIndex").length(); i++)
					{
						if (!token[info.getJSONArray("cValueIndex").getInt(i)].equalsIgnoreCase(info.getJSONArray("cValue").getString(i)))
						{
							flag = false; break;
						}
					}
				}

				if (flag)
				{
					for (int index = 0; index < columns.length; index++)
					{
						if (index > 0) 
						{
							buffer.append(info.getString("separator"));
						}
						buffer.append(token[columns[index]]);
					}

					buffer.append("\n");

					counter.put("count", counter.getInt("count") + 1);

					return buffer.toString();
				}
				else
				{
					return buffer.append("");
				}
			}).collect(Collectors.toList());

			bufferedReader.close();
			inputStream.close();

			if (counter.getInt("count") > 1) 
			{
				final String pathBaseToNewRepo = dataFileBaseDir.concat("/").concat(repo).concat("-dir/").concat(info.getString("locale").replace(":", "/"));

				final File file = new File(pathBaseToNewRepo);

				file.mkdirs();

				try (final PrintWriter printWriter = new PrintWriter(new File(pathBaseToNewRepo.concat("/").concat("all.feature")));) 
				{
					printWriter.write(buffer.toString());
					printWriter.close();
				}
			}
			else
			{
				throw new Exception("error: no data was filtered to persist to this locale.");
			}
		}
		catch (Exception e) 
		{
			throw e;
		}
		finally 
		{
			if (bufferedReader != null)
			{
				try 
				{
					bufferedReader.close();
				} 
				catch (IOException e) 
				{
					e.printStackTrace();
				}
			}

			if (inputStream != null)
			{
				try 
				{
					inputStream.close();
				} 
				catch (IOException e) 
				{
					e.printStackTrace();
				}
			}
		}
	}
	
	public static void main(String ... args) throws Exception
	{
		new DataManagerServiceImpl().slice("p971074907", new JSONObject());
	}
}
