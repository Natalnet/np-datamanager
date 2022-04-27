package com.np.datamanager.controllers;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.np.commons.model.Timeseries;
import com.np.commons.stats.StatisticsUtil;
import com.np.datamanager.Monitoring;
import com.np.datamanager.service.DataManagerService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@Api(description = "The purpose of this module is to manage storage and control access to raw data files "
		+ "retrieved from external repositories. These files are stored on the system locally by the system "
		+ "and are assigned an identification key. "
		+ "\n\n"
		+ "The only treatment that should be taken at this stage of the ML process can be the adequacy of "
		+ "column names (in case of files in CSV format). In addition to this treatment, another specific one "
		+ "must be given, that of semantically describing each of the column header names of the file (CSV). "
		+ "\n\n"
		+ "The description that should be given to header names should be put as the second line of the article "
		+ "and should use ';' as a field separator. all other lines must use ',' as field separator.")
@RequestMapping("/")
@RestController
public class DataManagerController
{
	@Value("${storage.path.base}")
	private String dataFileBaseDir;
	
	@Autowired
	private DataManagerService dataManagerService;
	
	@ApiOperation(value = "Use this to upload a raw file and store it in system. If this file has received "
			+ "any treatment, this treatment must be given only in the format of the name of the headers or "
			+ "in their description. If the file contains more than one column, it should be named 'all' "
			+ "(use url parameter 'feature' to do this). If it does not have all columns, then it must "
			+ "contain only one column and be named with the column header name.")
	@ApiImplicitParams({
		@ApiImplicitParam(name = "urlRepoKey", value = "the key URL repo from file uploaded", paramType = "String", required = true),
		@ApiImplicitParam(name = "path", value = "the path locale to a locale", paramType = "String", required = true, example = "brl:rn"),
		@ApiImplicitParam(name = "feature", value = "if the file contains a single feature, this param value is this feature name. if the file contains multiple features, this param value will be 'all'.", paramType = "String", required = true)
	})
	@ApiResponses({ 
		@ApiResponse(code = 201, message = "Success"), 
		@ApiResponse(code = 400, message = "Bad Request"),
		@ApiResponse(code = 404, message = "Not Found"),
		@ApiResponse(code = 417, message = "Expectation Failed"),
		@ApiResponse(code = 500, message = "Internal Server Error")
	})
	@PostMapping(path = "/repo/{urlRepoKey}/path/{path}/feature/{feature}", consumes = MediaType.MULTIPART_FORM_DATA_VALUE, produces = MediaType.TEXT_PLAIN_VALUE)
	public ResponseEntity<String> uploadFileBody(@PathVariable(required = true) String urlRepoKey,
			@PathVariable(required = true) String path,
			@PathVariable(required = true) String feature,
			@RequestParam("data") MultipartFile file) throws JSONException
	{
		try
		{
			dataManagerService.uploadRawDataAndStoreLocally(file, urlRepoKey, path.trim().replace(":", "/").toLowerCase(), feature);
			return ResponseEntity.status(HttpStatus.CREATED).build();
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return ResponseEntity.status(HttpStatus.EXPECTATION_FAILED).body(e.getMessage());
		}
	}
	
	@ApiOperation(value = "Download a raw file from external repository and store it locally.")
	@ApiImplicitParams({
		@ApiImplicitParam(name = "JSON Body", value = "Object to be created", paramType = "body", required = true, 
				example = "{ \"urlRepo\":\"<httpaddress>\" }")
	})
	@ApiResponses({ 
		@ApiResponse(code = 201, message = "Success"), 
		@ApiResponse(code = 400, message = "Bad Request"),
		@ApiResponse(code = 404, message = "Not Found"),
		@ApiResponse(code = 417, message = "Expectation Failed"),
		@ApiResponse(code = 500, message = "Internal Server Error")
	})
	@PostMapping(path = "/repo", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.TEXT_PLAIN_VALUE)
	public ResponseEntity<String> downloadRawDataAndStoreInFile(@RequestBody(required = true) String body) 
	{
		HttpStatus httpStatus = HttpStatus.EXPECTATION_FAILED;
		
		try
		{
			final JSONObject jsObject = new JSONObject(body);
			
			dataManagerService.downloadRawDataAndStoreLocally(jsObject.getString("urlRepo"));
			return ResponseEntity.status(HttpStatus.CREATED).build();
		} 
		catch (Exception e) 
		{
			if (e.getMessage() == null) 
			{
				httpStatus = HttpStatus.INTERNAL_SERVER_ERROR;
			}

			return ResponseEntity.status(httpStatus).body(e.getMessage());
		}
	}
	
	@ApiOperation(value = "List all raw file locally stored.")
	@ApiResponses({ 
		@ApiResponse(code = 200, message = "Success - response body: [{'urlRepoKey':String, 'urlRepo':String},*]"), 
		@ApiResponse(code = 404, message = "Not Found"),
		@ApiResponse(code = 417, message = "Expectation Failed"),
		@ApiResponse(code = 500, message = "Internal Server Error")
	})
	@GetMapping(path = "/repo", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> listRawDataFiles() 
	{
		HttpStatus httpStatus = HttpStatus.EXPECTATION_FAILED;
		
		try 
		{
			return ResponseEntity.ok(dataManagerService.listAllRawDatas().toString());
		} 
		catch (Exception e) 
		{
			if (e.getMessage() == null) 
			{
				httpStatus = HttpStatus.INTERNAL_SERVER_ERROR;
			}
			return ResponseEntity.status(httpStatus).body(e.getMessage());
		}
	}
	
	@ApiOperation(value = "List all stored file for a repository.")
	@ApiImplicitParams({
		@ApiImplicitParam(name = "urlRepoKey", value = "the key URL repo from file uploaded", paramType = "String", required = true),
	})
	@ApiResponses({ 
		@ApiResponse(code = 200, message = "Success - response body: { 'brl': { 'rn': { 'natal': { 'files': ['all.feature'] }, 'files': ['all.feature', 'cases.feature']} } }"), 
		@ApiResponse(code = 404, message = "Not Found"),
		@ApiResponse(code = 417, message = "Expectation Failed"),
		@ApiResponse(code = 500, message = "Internal Server Error")
	})
	@GetMapping(path = "/repo/{urlRepoKey}/list", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> listDataFilesByRepo(@PathVariable(required = true, name = "urlRepoKey") String urlRepoKey) 
	{
		HttpStatus httpStatus = HttpStatus.EXPECTATION_FAILED;
		
		try 
		{
			LoggerFactory.getLogger(getClass()).info(urlRepoKey);
			
			return ResponseEntity.ok(dataManagerService.listDatasByRepo(urlRepoKey).toString());
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
			
			if (e.getMessage() == null) 
			{
				httpStatus = HttpStatus.INTERNAL_SERVER_ERROR;
			}
			return ResponseEntity.status(httpStatus).body(e.getMessage());
		}
	}
	
	@ApiOperation(value = "Get all body data file as JSON string.")
	@ApiImplicitParams({
		@ApiImplicitParam(name = "urlRepoKey", value = "the key URL repo from file uploaded", paramType = "String", required = true),
		@ApiImplicitParam(name = "path", value = "the path locale to a locale", paramType = "String", required = true, example = "brl:rn"),
		@ApiImplicitParam(name = "feature", value = "a colon-separated list of feature names.", paramType = "String", required = true, example = "'cases:death' or you can use just 'all'")
	})
	@ApiResponses({ 
		@ApiResponse(code = 200, message = "Success - response body: [{'columnName':String, 'columnValue':String},*]"), 
		@ApiResponse(code = 404, message = "Not Found"),
		@ApiResponse(code = 417, message = "Expectation Failed"),
		@ApiResponse(code = 500, message = "Internal Server Error")
	})
	@GetMapping(value = "/repo/{urlRepoKey}/path/{path}/feature/{feature}/as-json", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> getDataFileAsJSON(@PathVariable(required = true) String urlRepoKey,
			@PathVariable(required = true) String path, @PathVariable(required = true) String feature)
	{
		try
		{
			final JSONArray jsArray = dataManagerService.getDataAsJSON(urlRepoKey, path.trim().replace(":", "/").toLowerCase(), feature.split(Pattern.quote(":")));
			if (jsArray == null)
			{
				return ResponseEntity.status(HttpStatus.NOT_FOUND).body("");
			}
			else
			{
				return ResponseEntity.status(HttpStatus.OK).body(jsArray.toString());
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return ResponseEntity.status(HttpStatus.EXPECTATION_FAILED).body(e.getMessage());
		}
	}

	@ApiOperation(value = "Get body data file as JSON string by period.")
	@ApiImplicitParams({
		@ApiImplicitParam(name = "urlRepoKey", value = "the key URL repo from file uploaded", paramType = "String", required = true),
		@ApiImplicitParam(name = "path", value = "the path locale to a locale", paramType = "String", required = true, example = "brl:rn"),
		@ApiImplicitParam(name = "feature", value = "a colon-separated list of feature names.", paramType = "String", required = true, example = "'cases:death' or you can use just 'all'"),
		@ApiImplicitParam(name = "begin", paramType = "String", required = true, example = "2020-06-02"),
		@ApiImplicitParam(name = "end", paramType = "String", required = true, example = "2020-06-09")
	})
	@ApiResponses({ 
		@ApiResponse(code = 200, message = "Success - response body: [{'columnName':String, 'columnValue':String},*]"), 
		@ApiResponse(code = 404, message = "Not Found"),
		@ApiResponse(code = 417, message = "Expectation Failed"),
		@ApiResponse(code = 500, message = "Internal Server Error")
	})
	@GetMapping(value = "/repo/{urlRepoKey}/path/{path}/feature/{feature}/begin/{begin}/end/{end}/as-json", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> getDataFileAsStringByBeginAndEnd(@PathVariable(required = true) String urlRepoKey,
			@PathVariable(required = true) String path, @PathVariable(required = true) String feature, 
			@PathVariable(required = true) String begin, @PathVariable(required = true) String end)
	{
		try
		{
			final JSONArray jsArray = dataManagerService.getDataAsJSON(urlRepoKey, path.trim().replace(":", "/").toLowerCase(), feature.split(Pattern.quote(":")), begin, end);
			if (jsArray == null)
			{
				return ResponseEntity.status(HttpStatus.NOT_FOUND).body("");
			}
			else
			{
				return ResponseEntity.status(HttpStatus.OK).body(jsArray.toString());
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return ResponseEntity.status(HttpStatus.EXPECTATION_FAILED).body(e.getMessage());
		}
	}

	@ApiOperation(value = "Get body data file as CSV string by period.")
	@ApiImplicitParams({
		@ApiImplicitParam(name = "urlRepoKey", value = "the key URL repo from file uploaded", paramType = "String", required = true),
		@ApiImplicitParam(name = "path", value = "the path locale to a locale", paramType = "String", required = true, example = "brl:rn"),
		@ApiImplicitParam(name = "feature", value = "a colon-separated list of feature names.", paramType = "String", required = true, example = "'cases:death' or you can use just 'all'"),
		@ApiImplicitParam(name = "begin", paramType = "String", required = true, example = "2020-06-02"),
		@ApiImplicitParam(name = "end", paramType = "String", required = true, example = "2020-06-09")
	})
	@ApiResponses({ 
		@ApiResponse(code = 200, message = "Success - response body: a body CSV file format with comma-separeted list "
				+ "values (line by line). First line is header"), 
		@ApiResponse(code = 404, message = "Not Found"),
		@ApiResponse(code = 417, message = "Expectation Failed"),
		@ApiResponse(code = 500, message = "Internal Server Error")
	})
	@GetMapping(value = "/repo/{urlRepoKey}/path/{path}/feature/{feature}/begin/{begin}/end/{end}/as-csv", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> getDataFileAsCSVByBeginAndEnd(@PathVariable(required = true) String urlRepoKey,
			@PathVariable(required = true) String path, @PathVariable(required = true) String feature, 
			@PathVariable(required = true) String begin, @PathVariable(required = true) String end)
	{
		try
		{
			final String bodyFile = dataManagerService.getDataAsCSV(urlRepoKey, path.trim().replace(":", "/").toLowerCase(), feature.split(Pattern.quote(":")), begin, end);
			if (bodyFile == null)
			{
				return ResponseEntity.status(HttpStatus.NOT_FOUND).body("");
			}
			else
			{
				return ResponseEntity.status(HttpStatus.OK).body(bodyFile);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return ResponseEntity.status(HttpStatus.EXPECTATION_FAILED).body(e.getMessage());
		}
	}
	
	@ApiOperation(value = "Get header from a raw data file.")
	@ApiImplicitParams({
		@ApiImplicitParam(name = "urlRepoKey", value = "the key URL repo from file uploaded", paramType = "String", required = true),
		@ApiImplicitParam(name = "path", value = "the path locale to a locale", paramType = "String", required = true, example = "brl:rn"),
		@ApiImplicitParam(name = "feature", value = "a colon-separated list of feature names.", paramType = "String", required = true, example = "'cases:death' or you can use just 'all'")
	})
	@ApiResponses({ 
		@ApiResponse(code = 200, message = "Success - response body: a string containing the "
				+ "first five lines of the body of a CSV file, including the header."), 
		@ApiResponse(code = 404, message = "Not Found"),
		@ApiResponse(code = 417, message = "Expectation Failed"),
		@ApiResponse(code = 500, message = "Internal Server Error")
	})
	@GetMapping(value = "/repo/{urlRepoKey}/path/{path}", produces = MediaType.TEXT_PLAIN_VALUE)
	public ResponseEntity<String> getHeaderFromCSVFile(@PathVariable(required = true) String urlRepoKey,
			@PathVariable(required = true) String path)
	{
		try
		{
			final String bodyFile = dataManagerService.getDataFeatures(urlRepoKey, path.trim().replace(":", "/").toLowerCase());
			if (bodyFile == null)
			{
				return ResponseEntity.status(HttpStatus.NOT_FOUND).body("");
			}
			else
			{
				return ResponseEntity.status(HttpStatus.OK).body(bodyFile);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return ResponseEntity.status(HttpStatus.EXPECTATION_FAILED).body(e.getMessage());
		}
	}
	
	@ApiOperation(value = "Delete a raw data file for a repo (urlRepoKey).")
	@ApiImplicitParams({
		@ApiImplicitParam(name = "urlRepoKey", value = "the key URL repo from file uploaded", paramType = "String", required = true),
		@ApiImplicitParam(name = "path", value = "the path locale to a locale", paramType = "String", required = true, example = "brl:rn"),
		@ApiImplicitParam(name = "feature", value = "a colon-separated list of feature names.", paramType = "String", required = true, example = "'cases:death' or you can use just 'all'")
	})
	@ApiResponses({ 
		@ApiResponse(code = 200, message = "Success"), 
		@ApiResponse(code = 417, message = "Expectation Failed"),
		@ApiResponse(code = 500, message = "Internal Server Error")
	})
	@DeleteMapping(value = "/repo/{urlRepoKey}/path/{path}/feature/{feature}", produces = MediaType.TEXT_PLAIN_VALUE)
	public ResponseEntity<String> removeDataFile(@PathVariable(required = true) String urlRepoKey,
			@PathVariable(required = true) String path, @PathVariable(required = true) String feature) 
	{
		HttpStatus httpStatus = HttpStatus.EXPECTATION_FAILED;
		
		try 
		{
			dataManagerService.removeData(urlRepoKey, path.trim().replace(":", "/").toLowerCase(), feature);
			return ResponseEntity.ok("");
		} 
		catch (Exception e) 
		{
			if (e.getMessage() == null) 
			{
				httpStatus = HttpStatus.INTERNAL_SERVER_ERROR;
			}
			return ResponseEntity.status(httpStatus).body(e.getMessage());
		}
	}
	
	@ApiOperation(value = ".")
	@ApiImplicitParams({
		@ApiImplicitParam(name = "urlRepoKey", value = "the key URL repo from file uploaded", paramType = "String", required = true),
	})
	@ApiResponses({ 
		@ApiResponse(code = 201, message = "Success"), 
		@ApiResponse(code = 400, message = "Bad Request"),
		@ApiResponse(code = 404, message = "Not Found"),
		@ApiResponse(code = 417, message = "Expectation Failed"),
		@ApiResponse(code = 500, message = "Internal Server Error")
	})
	@PostMapping(path = "/repo/{urlRepoKey}/slice", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.TEXT_PLAIN_VALUE)
	public ResponseEntity<String> sliceCSV(@PathVariable(required = true) String urlRepoKey,
			@RequestBody(required = true) String body) throws JSONException
	{
		try
		{
			dataManagerService.slice(urlRepoKey, new JSONObject(body));
			
			return ResponseEntity.status(HttpStatus.CREATED).build();
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return ResponseEntity.status(HttpStatus.EXPECTATION_FAILED).body(e.getMessage());
		}
	}
	
	@ApiOperation(value = "use this endpoint to configure a repository's update schedule.")
	@ApiImplicitParams({
		@ApiImplicitParam(name = "JSON Body", value = "Object to be created", paramType = "body", required = true, 
				example = "{{\"p971074907\":{\"download-data\":{\"urlRepo\":\"https://raw.githubusercontent.com/wcota/covid19br/master/cases-brazil-states.csv\"},\"slice-data\":{\"locale\":\"brl:rn\",\"columns\":[0,1,3,5,6,7,8,9,10],\"separator\":\",\",\"connective\":\"and\",\"cValueIndex\":[3,4],\"cValue\":[\"RN\",\"TOTAL\"]}}}}")
	})
	@ApiResponses({ 
		@ApiResponse(code = 200, message = "Success"), 
		@ApiResponse(code = 417, message = "Expectation Failed"),
		@ApiResponse(code = 500, message = "Internal Server Error")
	})
	@PostMapping(path = "/repo/schedule", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.TEXT_PLAIN_VALUE)
	public ResponseEntity<String> setSchedule(@RequestBody(required = true) String body) 
	{
		/*
		{
		  "n0290999393": {
		    "download-data": {
		      "urlRepo": ""
		    },
		    "slide-data": {
			  "locale": "SP"
			  "columns": [],
			  "separator": ",",
			  "connective": "and",
			  "cValueIndex": [],
			  "cValue": []
			}
		  }
		}
		 */
		try 
		{
			Monitoring.getInstance(24l).getAgenda().setAppointments(new JSONObject(body));
			
			return ResponseEntity.ok().body("");
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
			return ResponseEntity.status(HttpStatus.EXPECTATION_FAILED).body(e.getMessage());
		}
	}
	
	@ApiOperation(value = "Use this endpoint to retrieve the list of scheduled updates from the repositories.")
	@ApiResponses({ 
		@ApiResponse(code = 200, message = "Success"), 
		@ApiResponse(code = 417, message = "Expectation Failed"),
		@ApiResponse(code = 500, message = "Internal Server Error")
	})
	@GetMapping(path = "/repo/schedule", produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<String> getSchedule() 
	{
		try 
		{
			return ResponseEntity.ok().body(Monitoring.getInstance(24l).getAgenda().getAppointments().toString(2));
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
			return ResponseEntity.status(HttpStatus.EXPECTATION_FAILED).body(e.getMessage());
		}
	}
    @ApiOperation(value = "Get mean.")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "urlRepoKey", value = "the key URL repo from file uploaded", paramType = "String", required = true),
            @ApiImplicitParam(name = "path", value = "the path locale to a locale", paramType = "String", required = true, example = "brl:rn"),
            @ApiImplicitParam(name = "feature", value = "a colon-separated list of feature names.", paramType = "String", required = true, example = "'cases'"),
            @ApiImplicitParam(name = "begin", paramType = "String", required = true, example = "2020-06-02"),
            @ApiImplicitParam(name = "end", paramType = "String", required = true, example = "2020-06-09"),
            @ApiImplicitParam(name = "period", paramType = "the Integer value representing the period for calculating the moving average", required = true, example = "7")
    })
    @ApiResponses({
            @ApiResponse(code = 200, message = "Success - response body: [{'columnName':String, 'columnValue':String},*]"),
            @ApiResponse(code = 404, message = "Not Found"), @ApiResponse(code = 417, message = "Expectation Failed"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    @GetMapping(value = "/repo/{urlRepoKey}/path/{path}/feature/{features}/period/{period}/as-json", produces = MediaType.TEXT_PLAIN_VALUE)
    public ResponseEntity<String> getMovingAverageAsJSON(@PathVariable(required = true) String urlRepoKey,
            @PathVariable(required = true) String path, @PathVariable(required = true) String features, 
            @PathVariable(required = true) Integer period)
    {
        try
        {
            final String fileFullName = Paths.get(dataFileBaseDir.concat("/").concat(urlRepoKey).concat("-dir/").concat(path.replace(":", "/")))
                    .toFile().getPath().concat("/avg.p").concat(String.valueOf(period)).concat("all.feature");
            
            final Timeseries timeseries;
            if (new File(fileFullName).exists()) 
            {
                timeseries = dataManagerService.getMovingAverage(fileFullName);
            }
            else
            {
                timeseries = new StatisticsUtil(period)
                        .getMovingAverage(dataManagerService.getDataAsTimeseries(urlRepoKey, path, features.split(Pattern.quote(":"))), period);
                dataManagerService.saveMovingAverage(fileFullName, timeseries);
            }
            
            return ResponseEntity.status(HttpStatus.OK).body("");
        }
        catch (Exception e)
        {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.EXPECTATION_FAILED).body(e.getMessage());
        }
    }
}
