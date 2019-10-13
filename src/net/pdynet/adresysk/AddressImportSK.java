package net.pdynet.adresysk;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.Normalizer;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.DataSource;
import javax.xml.bind.DatatypeConverter;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

public class AddressImportSK {
	
	private Properties properties = null;
	private DataSource dataSource = null;
	private Connection connection = null;
	private CSVFormat csvFormat = null;
	
	private Map<String, Integer> zipCodes = null;
	private Map<String, Integer> cities = null;
	private Map<String, Integer> cityParts = null;
	private Map<String, Integer> streets = null;
	private AtomicInteger objectCounter = null;
	private Calendar importStarted = null;
	private Pattern dateTimePattern = Pattern.compile("(\\d+)\\.\\s*(\\d+)\\.\\s*(\\d+)\\s+(\\d+):(\\d+):(\\d+)");
	
	private List<String> dataTables;
	private List<String> tempTables;
	
	private Map<Integer, String> sourceDataCity = null;
	private Map<Integer, String> sourceDataStreet = null;
	private Map<Integer, String> sourceDataCityPart = null;
	private Map<Integer, BuildingRecord> sourceDataBuilding = null;
	private Set<Integer> storedPlaces = null;
	
	private static final String SQLTABLE_PLACES = "sk_places";
	private static final String SQLTABLE_CITY_PARTS = "sk_city_parts";
	private static final String SQLTABLE_CITIES = "sk_cities";
	private static final String SQLTABLE_ZIP = "sk_zip";
	private static final String SQLTABLE_STREETS = "sk_streets";
	private static final String SQLTABLE_RELATIONS = "sk_relations";
	
	private static final String SQLTABLE_PLACES_TEMP = "sk_places_temp";
	private static final String SQLTABLE_CITY_PARTS_TEMP = "sk_city_parts_temp";
	private static final String SQLTABLE_CITIES_TEMP = "sk_cities_temp";
	private static final String SQLTABLE_ZIP_TEMP = "sk_zip_temp";
	private static final String SQLTABLE_STREETS_TEMP = "sk_streets_temp";
	private static final String SQLTABLE_RELATIONS_TEMP = "sk_relations_temp";
	
	public void run(String[] args) {
		try {
			importStarted = Calendar.getInstance();
			
			dataTables = Arrays.asList(
					SQLTABLE_PLACES, SQLTABLE_CITY_PARTS,
					SQLTABLE_CITIES, SQLTABLE_ZIP,
					SQLTABLE_STREETS, SQLTABLE_RELATIONS);
			
			tempTables = Arrays.asList(
					SQLTABLE_PLACES_TEMP, SQLTABLE_CITY_PARTS_TEMP,
					SQLTABLE_CITIES_TEMP, SQLTABLE_ZIP_TEMP,
					SQLTABLE_STREETS_TEMP, SQLTABLE_RELATIONS_TEMP);
			
			File configFile = getConfigFile();
			loadProperties(configFile);
			dataSource = getDataSource();
			
			try {
				connection = dataSource.getConnection();
				
				zipCodes = new HashMap<String, Integer>();
				cities = new HashMap<String, Integer>();
				cityParts = new HashMap<String, Integer>();
				streets = new HashMap<String, Integer>();
				objectCounter = new AtomicInteger();
				
				sourceDataCity = new HashMap<>();
				sourceDataStreet = new HashMap<>();
				sourceDataCityPart = new HashMap<>();
				sourceDataBuilding = new HashMap<>();
				storedPlaces = new HashSet<>();
				
				csvFormat = CSVFormat.DEFAULT
						.withHeader()
						.withSkipHeaderRecord(false);
				
				//testCSVFile();
				
				parseSourceDataCity();
				parseSourceDataCityPart();
				parseSourceDataStreet();
				parseSourceDataBuilding();
				System.out.println(sourceDataBuilding.size());
				
				try {
					makeTempTables();
					importAddressPlaces();
					tempTablesToProduction();
				} catch (Exception e) {
					e.printStackTrace();
					dropTempTables();
				}
				
			} finally {
				try { if (connection != null) connection.close(); } catch (Exception x) {}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void testCSVFile() throws Exception {
		File csvFile = new File(properties.getProperty("datafile.region"));
		try (CSVParser csvParser = CSVParser.parse(csvFile, StandardCharsets.UTF_8, csvFormat)) {
			for (CSVRecord record : csvParser) {
				//
				//System.out.println(record);
				if (isCsvRecordTimeValid(record)) {
					String objectId = getCsvColumn(record, "objectId");
					System.out.println(objectId);
				}
			}
		}
	}
	
	protected void parseSourceDataCity() throws IOException {
		File csvFile = new File(properties.getProperty("datafile.city"));
		try (CSVParser csvParser = CSVParser.parse(csvFile, StandardCharsets.UTF_8, csvFormat)) {
			for (CSVRecord record : csvParser) {
				if (isCsvRecordTimeValid(record)) {
					String objectId = getCsvColumn(record, "objectId");
					String municipalityName = getCsvColumn(record, "municipalityName");
					if (StringUtils.isNotBlank(objectId) && StringUtils.isNotBlank(municipalityName))
						sourceDataCity.put(Integer.valueOf(objectId), municipalityName);
				}
			}
		}
	}
	
	protected void parseSourceDataCityPart() throws IOException {
		File csvFile = new File(properties.getProperty("datafile.citypart"));
		try (CSVParser csvParser = CSVParser.parse(csvFile, StandardCharsets.UTF_8, csvFormat)) {
			for (CSVRecord record : csvParser) {
				if (isCsvRecordTimeValid(record)) {
					String objectId = getCsvColumn(record, "objectId");
					String districtName = getCsvColumn(record, "districtName");
					if (StringUtils.isNotBlank(objectId) && StringUtils.isNotBlank(districtName))
						sourceDataCityPart.put(Integer.valueOf(objectId), districtName);
				}
			}
		}
	}
	
	protected void parseSourceDataStreet() throws IOException {
		File csvFile = new File(properties.getProperty("datafile.street"));
		try (CSVParser csvParser = CSVParser.parse(csvFile, StandardCharsets.UTF_8, csvFormat)) {
			for (CSVRecord record : csvParser) {
				if (isCsvRecordTimeValid(record)) {
					String objectId = getCsvColumn(record, "objectId");
					String streetName = getCsvColumn(record, "streetName");
					if (StringUtils.isNotBlank(objectId) && StringUtils.isNotBlank(streetName))
						sourceDataStreet.put(Integer.valueOf(objectId), streetName);
				}
			}
		}
	}
	
	protected void parseSourceDataBuilding() throws IOException {
		File csvFile = new File(properties.getProperty("datafile.building"));
		try (CSVParser csvParser = CSVParser.parse(csvFile, StandardCharsets.UTF_8, csvFormat)) {
			for (CSVRecord record : csvParser) {
				if (isCsvRecordTimeValid(record)) {
					String objectId = getCsvColumn(record, "objectId");
					
					String municipalityIdentifier = getCsvColumn(record, "municipalityIdentifier");
					if (StringUtils.equalsIgnoreCase(municipalityIdentifier, "None"))
						municipalityIdentifier = null;
					
					String districtIdentifier = getCsvColumn(record, "districtIdentifier");
					if (StringUtils.equalsIgnoreCase(districtIdentifier, "None"))
						districtIdentifier = null;
					
					String propertyRegistrationNumber = getCsvColumn(record, "propertyRegistrationNumber");
					if (StringUtils.equalsIgnoreCase(propertyRegistrationNumber, "None") || StringUtils.equals(propertyRegistrationNumber, "0"))
						propertyRegistrationNumber = null;
					
					if (StringUtils.isNotBlank(objectId)) {
						Integer recId = Integer.valueOf(objectId);
						BuildingRecord rec = new BuildingRecord(
								recId,
								municipalityIdentifier == null ? null : Integer.valueOf(municipalityIdentifier),
								districtIdentifier == null ? null : Integer.valueOf(districtIdentifier),
								propertyRegistrationNumber);
						/*
						Integer ii = new Integer(objectId);
						if (sourceDataBuilding.containsKey(ii)) {
							System.out.println("duplicite record:");
							System.out.println(rec);
						}
						*/
						sourceDataBuilding.put(recId, rec);
						/*
						System.out.println(rec);
						if (sourceDataBuilding.size() > 5)
							break;
						*/
					}
				}
			}
		}
	}
	
	protected void importAddressPlaces() throws Exception {
		File csvFile = new File(properties.getProperty("datafile.entrance"));
		try (CSVParser csvParser = CSVParser.parse(csvFile, StandardCharsets.UTF_8, csvFormat)) {
			int counter = 0;
			for (CSVRecord record : csvParser) {
				if (isCsvRecordTimeValid(record)) {
					importAddressPlace(record);
					
					// TODO vyhodit
					//if (++counter > 20)
					//	break;
				}
				
				if ((++counter % 2000) == 0) {
					System.out.println("Zpracovano " + counter + " zaznamu z CSV");
				}
			}
		}
	}
	
	protected void importAddressPlace(CSVRecord record) throws Exception {
		// ID adresniho mista.
		Integer placeId = Integer.valueOf(getCsvColumn(record, "objectId"));
		
		// Zaznam je jiz ulozen.
		if (storedPlaces.contains(placeId))
			return;
		
		// PSC.
		String zipCode = getCsvColumn(record, "postalCode").replaceAll("\\s+", "");
		if (StringUtils.equalsIgnoreCase(zipCode, "None"))
			zipCode = null;
		if (StringUtils.isBlank(zipCode))
			return;
		
		// ID budovy.
		String propertyRegistrationNumberIdentifier = getCsvColumn(record, "propertyRegistrationNumberIdentifier");
		if (!StringUtils.isNumeric(propertyRegistrationNumberIdentifier))
			propertyRegistrationNumberIdentifier = null;
		if (StringUtils.isBlank(propertyRegistrationNumberIdentifier))
			return;
		
		// ID ulice.
		String streetNameIdentifier = getCsvColumn(record, "streetNameIdentifier");
		if (!StringUtils.isNumeric(streetNameIdentifier))
			streetNameIdentifier = null;
		
		// Cislo orientacni.
		String buildingNumber = getCsvColumn(record, "buildingNumber");
		if (StringUtils.equalsIgnoreCase(buildingNumber, "None") || StringUtils.equals(buildingNumber, "0"))
			buildingNumber = null;
		
		// Zaznam budovy.
		BuildingRecord buildingRecord = sourceDataBuilding.get(Integer.valueOf(propertyRegistrationNumberIdentifier));
		if (buildingRecord == null) {
			//throw new IllegalStateException("Cant't find building " + propertyRegistrationNumberIdentifier + " for placeId " + placeId);
			//System.out.println("Cant't find building " + propertyRegistrationNumberIdentifier + " for placeId " + placeId);
			return;
		}
		
		// Cislo popisne.
		String propertyRegistrationNumber = buildingRecord.getPropertyRegistrationNumber();
		if (StringUtils.equals(propertyRegistrationNumber, "0"))
			propertyRegistrationNumber = null;
		
		// Nazev mesta.
		String city = null;
		Integer tempId = buildingRecord.getMunicipalityIdentifier();
		if (tempId != null)
			city = sourceDataCity.get(tempId);
		//System.out.println("city=" + city);
		
		// Nazev casti obce.
		String cityPart = null;
		tempId = buildingRecord.getDistrictIdentifier();
		if (tempId != null)
			cityPart = sourceDataCityPart.get(tempId);
		//System.out.println("cityPart=" + cityPart);
		
		// Nazev ulice.
		String street = null;
		if (streetNameIdentifier != null)
			street = sourceDataStreet.get(Integer.valueOf(streetNameIdentifier));
		//System.out.println("street=" + street);
		/*
		System.out.println("zip=" + zipCode);
		System.out.println("cislo popisne=" + propertyRegistrationNumber);
		System.out.println("cislo orientacni=" + buildingNumber);
		System.out.println(buildingRecord);
		*/
		
		if (city == null && cityPart == null && street == null) {
			//System.out.println("neplatne mesto nebo ulice pro id " + placeId);
			return;
		}
		
		if (propertyRegistrationNumber == null && buildingNumber == null) {
			//System.out.println("neplatne cislo budovy pro id " + placeId);
			return;
		}
		
		Map<String, Object> data = new HashMap<String, Object>();
		Set<Integer> objectIds = new LinkedHashSet<Integer>();
		int zipCodeId = -1;
		int cityId = -1;
		int cityPartId = -1;
		int streetId = -1;
		
		// Ulozeni PSC.
		String str = zipCode.toUpperCase();
		String ft;
		if (StringUtils.isNotBlank(zipCode)) {
			if (!zipCodes.containsKey(str)) {
				zipCodeId = objectCounter.incrementAndGet();
				zipCodes.put(str, zipCodeId);
				ft = prepareForFulltext(str);
				
				data.clear();
				data.put("ID", zipCodeId);
				data.put("ZIP", zipCode);
				data.put("FT", ft);
				insertRecord(SQLTABLE_ZIP_TEMP, data);
			} else {
				zipCodeId = zipCodes.get(str);
			}
		}
		
		if (zipCodeId != -1)
			objectIds.add(zipCodeId);
		
		// Obec.
		if (StringUtils.isNotBlank(city)) {
			str = city.toUpperCase();
			if (!cities.containsKey(str)) {
				cityId = objectCounter.incrementAndGet();
				cities.put(str, cityId);
				ft = prepareForFulltext(str);
				
				data.clear();
				data.put("ID", cityId);
				data.put("CITY", city);
				data.put("FT", ft);
				insertRecord(SQLTABLE_CITIES_TEMP, data);
			} else {
				cityId = cities.get(str);
			}
		}
		
		if (cityId != -1)
			objectIds.add(cityId);
		
		// Cast obce.
		// - ulozi se jen, kdyz cast obce obsahuje slovo, ktere neni
		//   obsazeno v nazvu obce.
		if (StringUtils.isNotBlank(cityPart)) {
			str = StringUtils.defaultString(city) + " - " + cityPart;
			str = str.toUpperCase();
			
			if (isValidCityPart(city, cityPart)) {
				if (!cityParts.containsKey(str)) {
					cityPartId = objectCounter.incrementAndGet();
					cityParts.put(str, cityPartId);
					str = StringUtils.defaultString(city) + " - " + cityPart;
					ft = prepareForFulltext(cityPart);
					
					data.clear();
					data.put("ID", cityPartId);
					data.put("CITY_PART", str);
					data.put("FT", ft);
					insertRecord(SQLTABLE_CITY_PARTS_TEMP, data);
				} else {
					cityPartId = cityParts.get(str);
				}
			}
		}
		
		if (cityPartId != -1)
			objectIds.add(cityPartId);
		
		// Ulice.
		int priority = 1;
		str = street;
		if (StringUtils.isBlank(str)) {
			str = cityPart;
			priority++;
		}
		if (StringUtils.isBlank(str)) {
			str = city;
			priority++;
		}
		
		if (StringUtils.isNotBlank(str)) {
			if (!streets.containsKey(str.toUpperCase())) {
				streetId = objectCounter.incrementAndGet();
				streets.put(str.toUpperCase(), streetId);
				ft = prepareForFulltext(str);
				
				data.clear();
				data.put("ID", streetId);
				data.put("STREET", str);
				data.put("FT", ft);
				data.put("PRIORITY", priority);
				insertRecord(SQLTABLE_STREETS_TEMP, data);
			} else {
				streetId = streets.get(str.toUpperCase());
			}
		}
		
		if (streetId != -1)
			objectIds.add(streetId);
		
		// Adresni misto.
		String placeAddress = street;
		
		if (StringUtils.isBlank(placeAddress))
			placeAddress = cityPart;
		
		if (StringUtils.isBlank(placeAddress))
			placeAddress = city;
		
		String houseNumber = propertyRegistrationNumber;
		String oriNumber = buildingNumber;
		
		if (StringUtils.isNotBlank(placeAddress)) {
			data.clear();
			
			ft = prepareForFulltext(placeAddress);
			
			if (StringUtils.isNotBlank(houseNumber)) {
				placeAddress += " " + houseNumber;
				ft += " " + prepareForFulltext(houseNumber);
			}
			
			// Cislo orientacni.
			//
			if (StringUtils.isNotBlank(oriNumber)) {
				if (StringUtils.isBlank(houseNumber))
					placeAddress += " " + oriNumber;
				else
					placeAddress += "/" + oriNumber;
				
				str = oriNumber.toUpperCase();
				while (str.length() < 4)
					str += "_";
				
				ft += " " + str;
			}
			
			String ftStreet = ft;
			String ftCity = StringUtils.EMPTY;
			
			// Doplneni casti obce do FT.
			if (StringUtils.isNotBlank(cityPart) && isValidCityPart(city, cityPart)) {
				ft += " " + prepareForFulltext(cityPart);
				ftCity += " " + prepareForFulltext(cityPart);
			}
			
			// Doplneni obce do FT.
			if (StringUtils.isNotBlank(city)) {
				ft += " " + prepareForFulltext(city);
				ftCity += " " + prepareForFulltext(city);
			}
			
			Set<String> fts = new LinkedHashSet<String>();
			Arrays.stream(ft.split("\\s+"))
				.distinct()
				.forEach(x -> fts.add(x));
			ft = StringUtils.join(fts, " ").trim();
			
			fts.clear();
			Arrays.stream(ftStreet.split("\\s+"))
				.distinct()
				.forEach(x -> fts.add(x));
			ftStreet = StringUtils.join(fts, " ").trim();
			
			fts.clear();
			Arrays.stream(ftCity.split("\\s+"))
				.distinct()
				.forEach(x -> fts.add(x));
			ftCity = StringUtils.join(fts, " ").trim();
			
			// GPS souradnice.
			String lat = getCsvColumn(record, "axisB");
			String lon = getCsvColumn(record, "axisL");
			if (StringUtils.equalsIgnoreCase(lat, "None"))
				lat = null;
			if (StringUtils.equalsIgnoreCase(lon, "None"))
				lon = null;
			if (StringUtils.isNotBlank(lat) && StringUtils.isNotBlank(lon)) {
				data.put("LAT", Double.valueOf(lat));
				data.put("LON", Double.valueOf(lon));
			}
			
			data.put("ID", placeId);
			data.put("STREET1", placeAddress);
			data.put("STREET2", "");
			data.put("CITY", StringUtils.defaultString(city));
			data.put("ZIP", zipCode);
			data.put("FT", ft);
			data.put("FT_STREET", ftStreet);
			data.put("FT_CITY", ftCity);
			
			insertRecord(SQLTABLE_PLACES_TEMP, data);
			
			// Doplneni vazeb.
			insertRelations(placeId, objectIds);
			
			// Ulozeni ID zpracovaneho zaznamu.
			storedPlaces.add(placeId);
		}
		
	}
	
	protected boolean isCsvRecordTimeValid(CSVRecord record) {
		String validTo = getCsvColumn(record, "validTo");
		
		if (StringUtils.isBlank(validTo) || StringUtils.equalsIgnoreCase(validTo, "None"))
			return true;
		
		Calendar cal = null;
		
		Matcher m = dateTimePattern.matcher(validTo);
		if (m.matches())
			cal = parseDateTime(m);
		else
			cal = parseXmlDateTime(validTo);
		
		if (cal == null)
			return true;
		
		return importStarted.before(cal);
	}
	
	protected Calendar parseXmlDateTime(String dateTimeString) {
		return DatatypeConverter.parseDateTime(dateTimeString);
	}
	
	protected Calendar parseDateTime(Matcher m) {
		// 31. 12. 3000 0:00:00
		// 4. 5. 2016 0:00:00
		// 1. 5. 2004 1:59:59
		GregorianCalendar gc = null;
		
		// year, month, dayOfMonth, hour, minute, second
		LocalDateTime ldt = LocalDateTime.of(Integer.parseInt(m.group(3)),
				Integer.parseInt(m.group(2)),
				Integer.parseInt(m.group(1)),
				Integer.parseInt(m.group(4)),
				Integer.parseInt(m.group(5)),
				Integer.parseInt(m.group(6)));
		
		gc = GregorianCalendar.from(ZonedDateTime.of(ldt, ZoneId.systemDefault()));
		
		return gc;
	}
	
	protected boolean isValidCityPart(String city, String cityPart) {
		if (StringUtils.isBlank(city) && StringUtils.isNotBlank(cityPart))
			return true;
		
		boolean isValid = false;
		city = alphanum(city.toUpperCase());
		cityPart = alphanum(cityPart.toUpperCase());
		
		List<String> listCity = Arrays.asList(words(city));
		List<String> listParts = Arrays.asList(words(cityPart));
		
		if (!listCity.isEmpty() && !listParts.isEmpty()) {
			// Shoda 1. slova.
			if (StringUtils.equalsIgnoreCase(listCity.get(0), listParts.get(0)))
				return false;
		}
		
		Set<String> setCity = new HashSet<String>(listCity);
		Set<String> setParts = new HashSet<String>(listParts);
		
		for (String s : setParts) {
			if (!setCity.contains(s)) {
				isValid = true;
				break;
			}
		}
		
		return isValid;
	}
	
	protected void insertRelations(int addressId, Set<Integer> objectIds) throws SQLException {
		String query = "INSERT INTO `" + SQLTABLE_RELATIONS_TEMP + "` SET `PLACE_ID` = ?, `OBJ_ID` = ?";
		try (PreparedStatement pst = connection.prepareStatement(query)) {
			pst.setInt(1, addressId);
			for (int objectId : objectIds) {
				pst.setInt(2, objectId);
				pst.executeUpdate();
			}
		}
	}
	
	protected void insertRecord(String table, Map<String, Object> data) throws SQLException {
		StringBuilder query = new StringBuilder("INSERT INTO `");
		query.append(table);
		query.append("` SET");
		
		int counter = 0;
		for (String key : data.keySet()) {
			query.append(" `");
			query.append(key);
			query.append("` = ?");
			if (++counter < data.size())
				query.append(",");
		}
		
		try (PreparedStatement pst = connection.prepareStatement(query.toString())) {
			counter = 1;
			for (String key : data.keySet()) {
				//String val = data.get(key);
				Object val = data.get(key);
				
				if (val == null)
					pst.setNull(counter, java.sql.Types.NULL);
				else if (val instanceof Integer)
					pst.setInt(counter, (int) val);
				else if (val instanceof Long)
					pst.setLong(counter, (long) val);
				else if (val instanceof BigDecimal)
					pst.setBigDecimal(counter, (BigDecimal) val);
				else if (val instanceof Double)
					pst.setDouble(counter, (double) val);
				else if (val instanceof Float)
					pst.setFloat(counter, (float) val);
				else if (val instanceof Timestamp)
					pst.setTimestamp(counter, (Timestamp) val);
				else if (val instanceof Calendar)
					pst.setTimestamp(counter, new Timestamp(((Calendar) val).getTimeInMillis()));
				else if (val instanceof String)
					pst.setString(counter, (String) val);
				else
					throw new IllegalStateException("Unsupported data type: " + val.getClass().getName());
				
				counter++;
			}
			
			pst.executeUpdate();
		}
	}
	
	protected String getCsvColumn(CSVRecord record, String column) {
		String val = StringUtils.trimToEmpty(record.get(column));
		return val;
	}
	
	protected String unaccent(String s) {
		String temp = Normalizer.normalize(s, Normalizer.Form.NFD);
		return temp.replaceAll("\\p{InCombiningDiacriticalMarks}+", "");
		//return temp.replaceAll("[^\\p{ASCII}]", "");
	}
	
	protected String alphanum(String s) {
		return s.replaceAll("[^\\p{L}\\p{N}]", " ");
	}
	
	protected String[] words(String s) {
		return s.split("\\s+");
	}
	
	protected String prepareForFulltext(String s) {
		s = unaccent(s);
		s = alphanum(s);
		String[] parts = s.split("(?<=\\d)(?=\\p{L})|(?<=\\p{L})(?=\\d)");
		s = StringUtils.join(parts, " ");
		parts = words(s);
		
		Set<String> set = new LinkedHashSet<String>();
		
		for (String part : parts) {
			String temp = StringUtils.trimToEmpty(part.toUpperCase());
			
			if (StringUtils.isNotBlank(temp)) {
				while (temp.length() < 4)
					temp += "_";
				
				set.add(temp);
			}
		}
		
		s = StringUtils.join(set, " ");
		
		return s;
	}
	
	protected DataSource getDataSource() {
		MysqlDataSource ds = new MysqlDataSource();
		ds.setURL(properties.getProperty("datasource.url"));
		ds.setUser(properties.getProperty("datasource.username"));
		ds.setPassword(properties.getProperty("datasource.password"));
		
		return ds;
	}
	
	protected File getConfigFile() {
		File config = new File(System.getProperty("user.dir"), "app.ini");
		return config;
	}
	
	protected void loadProperties(File configFile) throws IOException {
		try (FileReader reader = new FileReader(configFile)) {
			properties = new Properties();
			properties.load(reader);
		}
	}
	
	protected void makeTempTables() throws SQLException {
		try (Statement stmt = connection.createStatement()) {
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < tempTables.size(); i++) {
				sb.setLength(0);
				sb.append("CREATE TABLE `");
				sb.append(tempTables.get(i));
				sb.append("` LIKE `");
				sb.append(dataTables.get(i));
				sb.append("`;");
				stmt.executeUpdate(sb.toString());
			}
		}
	}
	
	protected void tempTablesToProduction() throws SQLException {
		try (Statement stmt = connection.createStatement()) {
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < dataTables.size(); i++) {
				sb.setLength(0);
				sb.append("DROP TABLE `");
				sb.append(dataTables.get(i));
				sb.append("`;");
				stmt.executeUpdate(sb.toString());
				sb.setLength(0);
				sb.append("RENAME TABLE `");
				sb.append(tempTables.get(i));
				sb.append("` TO `");
				sb.append(dataTables.get(i));
				sb.append("`;");
				stmt.executeUpdate(sb.toString());
			}
		}
	}
	
	protected void dropTempTables() {
		try {
			try (Statement stmt = connection.createStatement()) {
				StringBuilder sb = new StringBuilder();
				for (int i = 0; i < tempTables.size(); i++) {
					sb.setLength(0);
					sb.append("DROP TABLE IF EXISTS `");
					sb.append(tempTables.get(i));
					sb.append("`;");
					stmt.executeUpdate(sb.toString());
				}
			}
		} catch (Exception e) {}
	}
}
