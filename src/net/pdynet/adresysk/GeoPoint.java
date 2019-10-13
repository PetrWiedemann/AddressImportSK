package net.pdynet.adresysk;

import org.apache.commons.lang3.math.NumberUtils;

public class GeoPoint
{
	protected double longitude = 0;
	protected double latitude = 0;
	protected double altitude = 0;
	
	public GeoPoint() {
		this(0, 0, 0);
	}
	
	public GeoPoint(double lon, double lat) {
		this(lon, lat, 0);
	}
	
	public GeoPoint(double lon, double lat, double alt) {
		longitude = lon;
		latitude = lat;
		altitude = alt;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public boolean equals(Object obj) {
		if (!(obj instanceof GeoPoint)) return false;
		if (((GeoPoint) obj).getLongitude() != this.longitude) return false;
		if (((GeoPoint) obj).getLatitude() != this.latitude) return false;
		return true;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		return "GeoPoint[ " + longitude + ", " + latitude + ", " + altitude + "]";
	}
	
	// -----  -----------------------------------------------------------------
	
	/**
	 * Nastaví longitude (zemepisna delka)
	 * @return
	 */
	public double getLongitude() {
		return longitude;
	}

	/**
	 * Vraci longitude (zemepisna delka)
	 * @param longitude
	 */
	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}

	/**
	 * Vraci latitude (zemepisna sirka)
	 * @return
	 */
	public double getLatitude() {
		return latitude;
	}

	/**
	 * Nastaví longitude (zemepisna sirka)
	 * @param latitude
	 */
	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}

	/**
	 * Nastaví altitude (nadmorska vyska)
	 * @return
	 */
	public double getAltitude() {
		return altitude;
	}

	/**
	 * Vraci altitude (nadmorska vyska)
	 * @param altitude
	 */
	public void setAltitude(double altitude) {
		this.altitude = altitude;
	}
	
    /**
     * Metoda prepocita souradnice zadane ve formatu JSTK a prevede je na 
     * souradnice ve formatu WGS84, pouzitelne v GoogleMapách  
     * 
     * @param xS
     * @param yS
     * @return
     */
    public static GeoPoint jtskToWGS84(String xS, String yS) {
    	return jtskToWGS84(xS, yS, "220");
    }
    
    /**
     * Metoda prepocita souradnice zadane ve formatu JSTK a prevede je na 
     * souradnice ve formatu WGS84, pouzitelne v GoogleMapách  
     * 
     * @param xS
     * @param yS
     * @param hS
     * @return
     */
    public static GeoPoint jtskToWGS84(String xS, String yS, String hS) {
    	GeoPoint point = new GeoPoint(); 
    	try {
			// Přepočet vstupích údajů 
    		
			double X = NumberUtils.createDouble(xS); 
			double Y = NumberUtils.createDouble(yS);
			double H = NumberUtils.createDouble(hS);
    		
			// Vypocet zemepisnych souradnic z rovinnych souradnic 
			
			double a = 6377397.15508;
			double e = 0.081696831215303;
			double n = 0.97992470462083;
			double konst_u_ro = 12310230.12797036;
			double sinUQ = 0.863499969506341;
			double cosUQ = 0.504348889819882;
			double sinVQ = 0.420215144586493;
			double cosVQ = 0.907424504992097;
			double alfa = 1.000597498371542;
			double k = 1.003419163966575;
			double ro = Math.sqrt(X * X + Y * Y);
			double epsilon = 2 * Math.atan(Y / (ro + X));
			double D = epsilon / n;
			double S = 2 * Math.atan(Math.exp(1 / n * Math.log(konst_u_ro / ro))) - Math.PI / 2;
			double sinS = Math.sin(S);
			double cosS = Math.cos(S);
			double sinU = sinUQ * sinS - cosUQ * cosS * Math.cos(D);
			double cosU = Math.sqrt(1 - sinU * sinU);
			double sinDV = Math.sin(D) * cosS / cosU;
			double cosDV = Math.sqrt(1 - sinDV * sinDV);
			double sinV = sinVQ * cosDV - cosVQ * sinDV;
			double cosV = cosVQ * cosDV + sinVQ * sinDV;
			double Ljtsk = 2 * Math.atan(sinV / (1 + cosV)) / alfa;
			double t = Math.exp(2 / alfa * Math.log((1 + sinU) / cosU / k));
			double pom = (t - 1) / (t + 1);
			
			double sinB = pom;
			do {
				sinB = pom;
				pom = t * Math.exp(e * Math.log((1 + e * sinB) / (1 - e * sinB)));
				pom = (pom - 1) / (pom + 1);
			} while (Math.abs(pom - sinB) > 1e-15);

			double Bjtsk = Math.atan(pom / Math.sqrt(1 - pom * pom));
			
			// Pravoúhlé souřadnice ve S-JTSK
			
			a = 6377397.15508;
			double f_1 = 299.152812853;
			double e2 = 1 - (1 - 1 / f_1) * (1 - 1 / f_1);
			ro = a / Math.sqrt(1 - e2 * Math.sin(Bjtsk) * Math.sin(Bjtsk));
			double x = (ro + H) * Math.cos(Bjtsk) * Math.cos(Ljtsk);
			double y = (ro + H) * Math.cos(Bjtsk) * Math.sin(Ljtsk);
			double z = ((1 - e2) * ro + H) * Math.sin(Bjtsk);
			
			// Pravoúhlé souřadnice v WGS-84
			
			double dx = 570.69;
			double dy = 85.69;
			double dz = 462.84;
			double wz = -5.2611 / 3600 * Math.PI / 180;
			double wy = -1.58676 / 3600 * Math.PI / 180;
			double wx = -4.99821 / 3600 * Math.PI / 180;
			double m = 3.543e-6;
			double xn = dx + (1 + m) * (x + wz * y - wy * z);
			double yn = dy + (1 + m) * (-wz * x + y + wx * z);
			double zn = dz + (1 + m) * (wy * x - wx * y + z);

			// Geodetické souřadnice v systému WGS-84
			
			a = 6378137.0;
			f_1 = 298.257223563;
			double a_b = f_1 / (f_1 - 1);
			double p = Math.sqrt(xn * xn + yn * yn);
			e2 = 1 - (1 - 1 / f_1) * (1 - 1 / f_1);
			double theta = Math.atan(zn * a_b / p);
			double st = Math.sin(theta);
			double ct = Math.cos(theta);
			t = (zn + e2 * a_b * a * st * st * st) / (p - e2 * a * ct * ct * ct);
			double B = Math.atan(t);
			double L = 2 * Math.atan(yn / (p + xn));
			H = Math.sqrt(1 + t * t) * (p - a / Math.sqrt(1 + (1 - e2) * t * t));
			
			point.latitude = B / Math.PI * 180;
			point.longitude = L / Math.PI * 180;;
			point.altitude = Math.ceil((H) * 100) / 100;
			
			// Formát výstupních hodnot
			/*
			B = B / Math.PI * 180;
			String sirka = "N";
			if (B < 0) {
				B = -B;
				sirka = "S";
			}
			double stsirky = Math.floor(B);
			B = (B - stsirky) * 60;
			double minsirky = Math.floor(B);
			B = (B - minsirky) * 60;
			double vtsirky = Math.round(B * 1000) / 1000;
			sirka = sirka + stsirky + "°" + minsirky + "'" + vtsirky;
			L = L / Math.PI * 180;
			String delka = "E";
			if (L < 0) {
				L = -L;
				delka = "W";
			}
			;
			double stdelky = Math.floor(L);
			L = (L - stdelky) * 60;
			double mindelky = Math.floor(L);
			L = (L - mindelky) * 60;
			double vtdelky = Math.round(L * 1000) / 1000;
			delka = delka + stdelky + "°" + mindelky + "'" + vtdelky;

			String vyska = Math.round((H) * 100) / 100 + " m";
			System.out.println("sirka:" + sirka + ", delka: " + delka + ", vyska: " + vyska);
			*/
		} catch (Exception exc) {
			exc.printStackTrace();
		}
    	return point;
    }
	
}
