package com.np.datamanager;

public class Monitoring 
{
	private static Monitoring monitoring;
	
	private Agenda agenda;
	
	private Monitoring(Long period) 
	{
		agenda = new Agenda(period);
	}
	
	final static synchronized public Monitoring getInstance(Long period)
	{
		if (monitoring == null)
		{
			monitoring = new Monitoring(period);
		}
		
		return monitoring;
	}
	
	public Agenda getAgenda() 
	{
		return agenda;
	}
}
