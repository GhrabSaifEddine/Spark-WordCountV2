package com.ov.spark.training;

import java.util.Date;

import scala.Serializable;

public class SessionCatcher implements Serializable{
	
	private String ip;
	private Date dateAccess ;
	private String typeAccess;
	private String pageAccess;
	private String resultAccess;
	
	private Date dateFirstAccess ;
	private Date dateLastAccess ;
	private Long dateDureeAccess=0L ;
	private String resultMessageAccess;
	private Long nbURLvisite=1L ;
	private String listeURLVisite="";
	
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public Date getDateAccess() {
		return dateAccess;
	}
	public void setDateAccess(Date dateAccess) {
		this.dateAccess = dateAccess;
	}
	public String getTypeAccess() {
		return typeAccess;
	}
	public void setTypeAccess(String typeAccess) {
		this.typeAccess = typeAccess;
	}
	public String getPageAccess() {
		return pageAccess;
	}
	public void setPageAccess(String pageAccess) {
		this.pageAccess = pageAccess;
	}
	public String getResultAccess() {
		return resultAccess;
	}
	public void setResultAccess(String resultAccess) {
		this.resultAccess = resultAccess;
	}
	public Date getDateFirstAccess() {
		return dateFirstAccess;
	}
	public void setDateFirstAccess(Date dateFirstAccess) {
		this.dateFirstAccess = dateFirstAccess;
	}
	public Date getDateLastAccess() {
		return dateLastAccess;
	}
	public void setDateLastAccess(Date dateLastAccess) {
		this.dateLastAccess = dateLastAccess;
	}
	public Long getDateDureeAccess() {
		return dateDureeAccess;
	}
	public void setDateDureeAccess(Long dateDureeAccess) {
		this.dateDureeAccess = dateDureeAccess;
	}
	public String getResultMessageAccess() {
		return resultMessageAccess;
	}
	public void setResultMessageAccess(String resultMessageAccess) {
		this.resultMessageAccess = resultMessageAccess;
	}
	public Long getNbURLvisite() {
		return nbURLvisite;
	}
	public void setNbURLvisite(Long nbURLvisite) {
		this.nbURLvisite = nbURLvisite;
	}
	public String getListeURLVisite() {
		return listeURLVisite;
	}
	public void setListeURLVisite(String listeURLVisite) {
		this.listeURLVisite = listeURLVisite;
	}
	
	
	
}
