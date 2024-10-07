package com.resume.parser.serviceimpl.in;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.tika.exception.TikaException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.xml.sax.SAXException;
import java.sql.*;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.resume.parser.ResponseWrapper;
import com.resume.parser.ResumeParserProgram;
import com.resume.parser.service.in.ParserService;


@Service
public class ParserServiceImpl implements ParserService {

	@Autowired
	private ResumeParserProgram resumeParserProgram;

	@Override
	public ResponseWrapper parseResume(MultipartFile file) {
		String uploadedFolder = System.getProperty("user.dir");
		if (uploadedFolder != null && !uploadedFolder.isEmpty()) {
			uploadedFolder += "/Resumes/";
		} else
			throw new RuntimeException("User Directory not found");
		ResponseWrapper responseWrapper = null;
		File tikkaConvertedFile = null;
		byte[] bytes = null;
		try {
			bytes = file.getBytes();
		} catch (IOException exception) {
			throw new RuntimeException(exception.getMessage());
		}
		Path path = null;
		try {
			path = Paths.get(uploadedFolder + file.getOriginalFilename());
			if (!Files.exists(path.getParent()))
				Files.createDirectories(path.getParent());
			path = Files.write(path, bytes);
		} catch (IOException exception) {
			throw new RuntimeException(exception.getMessage());

		}
		try {
			tikkaConvertedFile = resumeParserProgram.parseToHTMLUsingApacheTikka(path.toAbsolutePath().toString());
		} catch (IOException | SAXException | TikaException exception) {
			throw new RuntimeException(exception.getMessage());

		}
		JSONObject parsedJSON = null;
		JSONParser jsonParser = new JSONParser();
		if (tikkaConvertedFile != null) {
			try {
				parsedJSON = resumeParserProgram.loadGateAndAnnie(tikkaConvertedFile);
				Gson gson = new GsonBuilder().setPrettyPrinting().create();
				gson.toJson(parsedJSON, new FileWriter("ParsedFile.json"));
				JSONObject jsonObject = (JSONObject)jsonParser.parse(new FileReader("C:/Users/ADMIN/Downloads/resume-parser-master/resume-parser-master/ParsedFile.json"));
				JSONArray jsonArray = (JSONArray)jsonObject.get("basics");
				Connection con = ConnectToDB();
				PreparedStatement pStatement = con.prepareStatement("INSERT INTO jsondata values (?, ?, ?)");
				for(Object object : jsonArray){
					JSONObject rec = (JSONObject) object;
					String firstname = (String) rec.get("firstName");
					String surname = (String) rec.get("surname");
					String email = (String) rec.get("email");
					pStatement.setString(1, firstname);
					pStatement.setString(2, surname);
					pStatement.setString(3, email);
					pStatement.executeUpdate();
					System.out.println("Records Inserted......");
				}
				
			} catch (Exception exception) {
				throw new RuntimeException(exception.getMessage());

			}
			responseWrapper = new ResponseWrapper();
			responseWrapper.setStatus(200);
			responseWrapper.setData(parsedJSON);
			responseWrapper.setMessage("Successfully parsed Resume!");
		}
		return responseWrapper;
	}

	public static Connection ConnectToDB() throws Exception{
		Class.forName("com.mysql.jdbc.Driver");
      	String mysqlUrl = "jdbc:mysql://localhost:3306/resumeparser";
      	Connection con = DriverManager.getConnection(mysqlUrl, "root", "root");
      	System.out.println("Connection established......");
		return con;
	}

}
