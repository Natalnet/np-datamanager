package com.np.datamanager.service;

import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.core.io.Resource;
import org.springframework.web.multipart.MultipartFile;

import com.np.commons.model.Timeseries;

public interface DataManagerService 
{
	public void uploadRawDataAndStoreLocally(MultipartFile file, String urlRepoKey, String localeCode, String feature) throws Exception;
	
	public void downloadRawDataAndStoreLocally(String urlRepoKey) throws Exception;
	
//	public void downloadDataAndStoreLocally(String urlRepoKey, String path, String feature) throws Exception;
	
	public JSONArray listAllRawDatas() throws Exception;
	
	public JSONObject listDatasByRepo(String urlRepoKey) throws Exception;
	
	public Resource getData(String urlRepoKey, String path, String feature) throws Exception;
	
	public JSONArray getDataAsJSON(String urlRepoKey, String path, String [] features) throws Exception;
	
	public JSONArray getDataAsJSON(String urlRepoKey, String path, String [] features, String begin, String end) throws Exception;
	
	public String getDataAsCSV(String urlRepoKeyKey, String path, String [] features) throws Exception;
	
	public String getDataAsCSV(String urlRepoKey, String path, String [] features, String begin, String end) throws Exception;

	public String getDataFeatures(String urlRepoKey, String path) throws Exception;

	public void removeData(String urlRepoKey, String path, String feature) throws Exception;
	
	public void slice(String repo, JSONObject info) throws Exception;
	
	public Timeseries getDataAsTimeseries(String urlRepoKey, String path, String [] features) throws Exception;
	
	public void saveMovingAverage(String fileFullName, Timeseries timeseries) throws Exception;
    
    public Timeseries getMovingAverage(String fileFullName) throws Exception;
}
