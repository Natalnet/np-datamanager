package com.np.datamanager;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Agenda extends TimerTask
{
	private Logger logger = LoggerFactory.getLogger(Agenda.class);
	
	final private Timer timer;
	private JSONObject appointments = new JSONObject();
	
	public Agenda(Long period) 
	{
		timer = new Timer(true);
		timer.scheduleAtFixedRate(this, 0, 1000L * 60L * 60L * period);
	}

	public void setAppointments(JSONObject appointments)
	{
		this.appointments = appointments;
	}
	
	public JSONObject getAppointments() 
	{
		return appointments;
	}

	@SuppressWarnings("deprecation")
	@Override
	public void run() 
	{
		Agenda.this.logger.info("> wake up Agenda ... ");
		Agenda.this.logger.info("> Appointments: "+appointments.keySet());
		
		appointments.keySet().forEach(urlRepoKey -> {
			try 
			{
				Agenda.this.logger.info("trying appointment ".concat(urlRepoKey).concat(" ..."));
				
				if (!appointments.getJSONObject(urlRepoKey).has("download"))
				{
					appointments.getJSONObject(urlRepoKey).put("download", new JSONObject());
				}
				final JSONObject download = appointments.getJSONObject(urlRepoKey).getJSONObject("download");
				
				HttpRequest request = HttpRequest.newBuilder()
					.uri(new URI("http://localhost:8082/repo"))
					.header("Content-Type", "application/json")
					.timeout(Duration.ofSeconds(30))
					.POST(HttpRequest.BodyPublishers.ofString(
						appointments.getJSONObject(urlRepoKey).getJSONObject("download-data").toString()))
					.build();
				
				HttpResponse<String> response = HttpClient.newHttpClient()
					.send(request, HttpResponse.BodyHandlers.ofString());
				if (response.statusCode() == 201) 
				{
					download.put(new Date().toLocaleString(), "ok");
					Agenda.this.logger.info(" ... appointment ".concat(urlRepoKey).concat(" download ok."));
					
					if (!appointments.getJSONObject(urlRepoKey).has("slice"))
					{
						appointments.getJSONObject(urlRepoKey).put("slice", new JSONObject());
					}
					final JSONObject slice = appointments.getJSONObject(urlRepoKey).getJSONObject("slice");
					
					final JSONObject data = appointments.getJSONObject(urlRepoKey).getJSONObject("slice-data");
					request = HttpRequest.newBuilder()
							.uri(new URI("http://localhost:8082/repo/".concat(urlRepoKey).concat("/slice")))
							.header("Content-Type", "application/json")
							.timeout(Duration.ofSeconds(30))
							.POST(HttpRequest.BodyPublishers.ofString(data.toString()))
							.build();
					
					response = HttpClient.newHttpClient()
						.send(request, HttpResponse.BodyHandlers.ofString());
					if (response.statusCode() == 201) 
					{
						slice.put(new Date().toLocaleString(), "ok");
						Agenda.this.logger.info(" ... appointment ".concat(urlRepoKey).concat(" slice ").concat(data.getString("locale")).concat(" ok."));
					}
					else
					{
						slice.put(new Date().toLocaleString(), "nok [".concat(response.body()).concat("]"));
						Agenda.this.logger.info(" ... appointment ".concat(urlRepoKey)
								.concat(" slice ").concat(data.getString("locale")).concat(" nok. [SC: ").concat(String.valueOf(response.statusCode()))
								.concat("] [Body: ").concat(response.body()).concat("]"));
					}
				}
				else
				{
					download.put(new Date().toLocaleString(), "nok [".concat(response.body()).concat("]"));
					Agenda.this.logger.info(" ... appointment ".concat(urlRepoKey)
							.concat(" download nok. [sc:").concat(String.valueOf(response.statusCode()))
							.concat("] [Body: ").concat(response.body()).concat("]"));
				}
			} 
			catch (URISyntaxException | IOException | InterruptedException e) 
			{
				e.printStackTrace();
				Agenda.this.logger.error("exception for appointment ".concat(urlRepoKey).concat(". ").concat(e.getMessage()));
			}
		});
	}
	
	/*
	public final void givenAcceptingAllCertificates_whenHttpsUrlIsConsumed_thenOk() 
			  throws GeneralSecurityException {
			    HttpComponentsClientHttpRequestFactory requestFactory = new HttpComponentsClientHttpRequestFactory();
			    CloseableHttpClient httpClient = (CloseableHttpClient) requestFactory.getHttpClient();

			    TrustStrategy acceptingTrustStrategy = (cert, authType) -> true;
			    SSLSocketFactory sf = new SSLSocketFactory(acceptingTrustStrategy, ALLOW_ALL_HOSTNAME_VERIFIER);
			    httpClient.getConnectionManager().getSchemeRegistry().register(new Scheme("https", 8443, sf));

			    ResponseEntity<String> response = new RestTemplate(requestFactory).
			      exchange(urlOverHttps, HttpMethod.GET, null, String.class);
			    assertThat(response.getStatusCode().value(), equalTo(200));
			}
	 * */
	
	/*
	 public void whenHttpsUrlIsConsumed_thenException() {
	    String urlOverHttps 
	      = "http://localhost:8443/httpclient-simple/api/bars/1";
	    ResponseEntity<String> response 
	      = new RestTemplate().exchange(urlOverHttps, HttpMethod.GET, null, String.class);
	    //assertThat(response.getStatusCode().value(), equalTo(200));
	    
	    if (response.getStatusCode().value() == 200)
	    {
	    	
	    }
	}
	 */
	
	/* 
	public void givenAcceptingAllCertificates_whenHttpsUrlIsConsumed_thenException() 
			  throws GeneralSecurityException {
			    HttpComponentsClientHttpRequestFactory requestFactory 
			      = new HttpComponentsClientHttpRequestFactory();
			    DefaultHttpClient httpClient
			      = (DefaultHttpClient) requestFactory.getHttpClient();
			    TrustStrategy acceptingTrustStrategy = (cert, authType) -> true
			    SSLSocketFactory sf = new SSLSocketFactory(
			      acceptingTrustStrategy, ALLOW_ALL_HOSTNAME_VERIFIER);
			    httpClient.getConnectionManager().getSchemeRegistry()
			      .register(new Scheme("https", 8443, sf));

			    String urlOverHttps
			      = "https://localhost:8443/httpclient-simple/api/bars/1";
			    ResponseEntity<String> response = new RestTemplate(requestFactory).
			      exchange(urlOverHttps, HttpMethod.GET, null, String.class);
			    assertThat(response.getStatusCode().value(), equalTo(200));
			}
	 * */
	
	/*
	public void givenAcceptingAllCertificatesUsing4_4_whenUsingRestTemplate_thenCorrect() 
			throws ClientProtocolException, IOException {
			    CloseableHttpClient httpClient
			      = HttpClients.custom()
			        .setSSLHostnameVerifier(new NoopHostnameVerifier())
			        .build();
			    HttpComponentsClientHttpRequestFactory requestFactory 
			      = new HttpComponentsClientHttpRequestFactory();
			    requestFactory.setHttpClient(httpClient);

			    ResponseEntity<String> response 
			      = new RestTemplate(requestFactory).exchange(
			      urlOverHttps, HttpMethod.GET, null, String.class);
			    //assertThat(response.getStatusCode().value(), equalTo(200));
			}
	 */
	
	public void stop() 
	{
		timer.cancel();
	}
}
