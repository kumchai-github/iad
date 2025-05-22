package com.vlink.iad.service;

import com.vlink.iad.entity.OfferingEntity;
import com.vlink.iad.entity.RegistrationEntity;
import com.vlink.iad.repository.OfferingRepository;
import com.vlink.iad.repository.RegistrationRepository;
import com.vlink.iad.service.AzureBlobService;
import com.vlink.iad.service.PgpService;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import com.opencsv.*;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.azure.storage.blob.*;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;
import com.azure.security.keyvault.secrets.models.KeyVaultSecret;

import java.io.*;
import java.nio.file.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


@Service
public class CsvExportService {

    @Value("${input.directory}")
    private String input_directory;
    @Value("${input.export.file.pattern.course}")
    private String input_export_file_pattern_course;
    @Value("${input.export.pvk.course}")
    private String input_export_privatekey_course;		
    @Value("${input.export.file.pattern.class}")
    private String input_export_file_pattern_class;	
    @Value("${input.export.pvk.class}")
    private String input_export_privatekey_class;		
    @Value("${input.export.file.pattern.registration}")
    private String input_export_file_pattern_registration;
    @Value("${input.export.pvk.registration}")
    private String input_export_privatekey_registration;		
    @Value("${input.export.file.pattern.transcript}")
    private String input_export_file_pattern_transcript;	
    @Value("${input.export.pvk.transcript}")
    private String input_export_privatekey_transcript;
    @Value("${input.map.path.course}")
    private String input_map_path_course;
    @Value("${input.map.path.class}")
    private String input_map_path_class;
    @Value("${input.map.path.registration}")
    private String input_map_path_registration;
    @Value("${input.map.path.transcript}")
    private String input_map_path_transcript;	
    @Value("${output.directory}")
    private String output_directory;
    @Value("${output.export.file.prefix}")
    private String output_export_file_prefix;
    @Value("${output.export.pbk}")
    private String output_export_publickey;	
    @Value("${log.directory}")
    private String log_directory;	
	
    private final OfferingRepository offeringRepository;	
    private final RegistrationRepository registrationRepository;
    private final PgpService pgpService;
	private final AzureBlobService azureblobService;
	
    @Autowired
    public CsvExportService(
		OfferingRepository offeringRepository,			
		RegistrationRepository registrationRepository,
		PgpService pgpService,
		AzureBlobService azureblobService		
		) 
	{
        this.offeringRepository = offeringRepository;			
        this.registrationRepository = registrationRepository;
        this.pgpService = pgpService;	
		this.azureblobService = azureblobService;		
    }
	
    public void exportCourseCsv(boolean encrypt) {
		String pattern=input_directory + "/" + input_export_file_pattern_course;
		String mapFile=input_map_path_course; 
		azureblobService.getMap(mapFile);
        String endpoint = String.format("https://%s.blob.core.windows.net", azureblobService.getenv_account());
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .credential(azureblobService.getTokenCredential())
                .endpoint(endpoint)
                .buildClient();
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(azureblobService.getenv_container());
        Pattern regexPattern = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);		
        containerClient.listBlobsByHierarchy(input_directory + "/").forEach(blobItem -> {
            if (regexPattern.matcher(blobItem.getName()).matches()) {
				azureblobService.downloadBlob(containerClient, blobItem.getName(), input_directory);				
				String fullPath = blobItem.getName();
				String fileName = "";
				String inputFile = "";
				String outputFile = "";
				if (azureblobService.checkSuffix(fullPath,".pgp")) {
					String privatekey=azureblobService.getVault(input_export_privatekey_course);
					fileName = azureblobService.trimPgpSuffix(azureblobService.getFileName(fullPath));				
					inputFile = azureblobService.trimPgpSuffix(fullPath);
					outputFile = String.format("%s/%s%s", output_directory, output_export_file_prefix, fileName);
					pgpService.decryptFile (fullPath, inputFile, privatekey, "");				
					exportCourseCsv(inputFile, outputFile, mapFile);        
				} else if (azureblobService.checkSuffix(fullPath,".csv")) {
					fileName = azureblobService.getFileName(fullPath);				
					inputFile = fullPath;
					outputFile = String.format("%s/%s%s", output_directory, output_export_file_prefix, fileName);
					exportCourseCsv(inputFile, outputFile, mapFile);			
				}
				if (encrypt) {
					String publickey=azureblobService.getVault(output_export_publickey);
					pgpService.encryptFile (outputFile, String.format("%s.pgp",outputFile), publickey);				
					azureblobService.uploadBlob(containerClient, output_directory, String.format("%s%s.pgp",output_export_file_prefix,fileName), String.format("%s.pgp",outputFile), true);			
				} else {
					azureblobService.uploadBlob(containerClient, output_directory, String.format("%s%s",output_export_file_prefix,fileName), outputFile, true);			
				}
			}
        });
    }		
	
	/*
    private void exportCourseCsv (String inputFile, String outputFile, String mapFile) {
        BufferedReader reader=null;
		BufferedWriter writer=null;
		List<MapField> mapFields=null;
		try {
            reader = Files.newBufferedReader(Paths.get(inputFile));
            System.out.printf("Process input file: %s%n", inputFile);
		} catch (IOException e)  {
            System.out.printf("Error reading input file: %s%n", inputFile);
			return;
        }
        try {
            writer = Files.newBufferedWriter(Paths.get(outputFile));
		} catch (IOException e)  {
            System.out.printf("Error create output file: %s%n", outputFile);
			return;
        }
        try {
            mapFields = GetMap(mapFile);
        } catch (IOException e) {
            System.out.printf("Error reading map file: %s%n", mapFile);
        }	
		try {
            String header = reader.readLine();
            if (header != null) {
				if (header.startsWith("\uFEFF")) {
					header = header.substring(1);
				}
				// Fix: Null check for mapFields
				if (mapFields == null) {
					System.out.printf("Cannot proceed: mapFields is null due to error in map file: %s%n", mapFile);
					return;
				}	
				if (validateHeader(mapFields, header)) {
					writer.write(header);
					writer.newLine();
					String line;
					String[] headers = header.split(",");
					int index=1, num_success=0, num_error=0;
					while ((line = reader.readLine()) != null) {
						++index;
						String[] columns = line.split(",", -1); // Split by comma, keep empty fields
						if (columns.length == headers.length) {
							// Trim all columns
							columns = Arrays.stream(columns).map(String::trim).toArray(String[]::new);
							// Fix: Null check for mapFields
							if (mapFields == null) {
								System.out.printf("Cannot proceed: mapFields is null due to error in map file: %s%n", mapFile);
								return;
							}							
							if (!validateColumn(index,mapFields,columns)) {
								++num_error;
							} else {
								// Lookup for courseId to replace value in columns[2]
								String courseId = getFirstCourseIdByExternalId(columns[2]);	
								if (courseId != null) {
									columns[2] = courseId;
								}	
								writer.write(String.join(",", columns));
								writer.newLine();								
								++num_success;
							}
						} else {
							System.out.printf("Line %d: Error number of column does NOT match%n", index);
							++num_error;
						}
					}
					System.out.printf("Process record(s) %d Total| %d Success| %d Error%n",num_success+num_error,num_success,num_error);
					//System.out.printf("Create output file: %s%n",outputFile);
				} else {
					System.out.println("Error file header does NOT match");
				}	
				writer.close();
				reader.close();				
            }			
		} catch (Exception e)  {
            System.out.printf("Error operation not completed%n");
			return;
        }
    }	
	*/
	
	private void exportCourseCsv(String inputFile, String outputFile, String mapFile) {
		List<MapField> mapFields = null;

		try (
			BufferedReader reader = Files.newBufferedReader(Paths.get(inputFile));
			BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputFile))
		) {
			System.out.printf("Process input file: %s%n", inputFile);

			try {
				mapFields = GetMap(mapFile);
			} catch (IOException e) {
				System.out.printf("Error reading map file: %s%n", mapFile);
				return;
			}

			String header = reader.readLine();
			if (header != null) {
				if (header.startsWith("\uFEFF")) {
					header = header.substring(1);
				}

				if (validateHeader(mapFields, header)) {
					writer.write(header);
					writer.newLine();
					String line;
					String[] headers = header.split(",");
					int index = 1, num_success = 0, num_error = 0;

					while ((line = reader.readLine()) != null) {
						++index;
						String[] columns = line.split(",", -1); // keep empty fields
						if (columns.length == headers.length) {
							columns = Arrays.stream(columns).map(String::trim).toArray(String[]::new);
							if (!validateColumn(index, mapFields, columns)) {
								++num_error;
							} else {
								String courseId = getFirstCourseIdByExternalId(columns[2]);
								if (courseId != null) {
									columns[2] = courseId;
									columns[7] = "9999-01-01";
								}
								writer.write(String.join(",", columns));
								writer.newLine();
								++num_success;
							}
						} else {
							System.out.printf("Line %d: Error number of column does NOT match%n", index);
							++num_error;
						}
					}
					System.out.printf("Process record(s) %d Total| %d Success| %d Error%n", 
						num_success + num_error, num_success, num_error);
				} else {
					System.out.println("Error file header does NOT match");
				}
			}
		} catch (IOException e) {
			System.out.printf("Error processing file: %s%n", e.getMessage());
		} catch (Exception e) {
			System.out.printf("Error operation not completed%n");
		}
	}	

    public void exportClassCsv(boolean encrypt) {
		String pattern=input_directory + "/" + input_export_file_pattern_class;
		String mapFile=input_map_path_class;  
		azureblobService.getMap(mapFile);		
        String endpoint = String.format("https://%s.blob.core.windows.net", azureblobService.getenv_account());
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .credential(azureblobService.getTokenCredential())
                .endpoint(endpoint)
                .buildClient();
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(azureblobService.getenv_container());
        Pattern regexPattern = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);		
        containerClient.listBlobsByHierarchy(input_directory + "/").forEach(blobItem -> {
            if (regexPattern.matcher(blobItem.getName()).matches()) {
				azureblobService.downloadBlob(containerClient, blobItem.getName(), input_directory);				
				String fullPath = blobItem.getName();
				String fileName = "";
				String fileNameWithoutExtension = "";
				String inputFile = "";
				String outputFile = "";
				if (azureblobService.checkSuffix(fullPath,".pgp")) {
					String privatekey=azureblobService.getVault(input_export_privatekey_class);
					fileName = azureblobService.trimPgpSuffix(azureblobService.getFileName(fullPath));
					fileNameWithoutExtension = azureblobService.trimCsvSuffix(fileName);
					inputFile = azureblobService.trimPgpSuffix(fullPath);
					outputFile = String.format("%s/%s%s_", output_directory, output_export_file_prefix, fileNameWithoutExtension);
					pgpService.decryptFile (fullPath, inputFile, privatekey, "");				
					exportClassCsv(inputFile, outputFile, mapFile, encrypt, containerClient);        
				} else if (azureblobService.checkSuffix(fullPath,".csv")) {
					fileName = azureblobService.getFileName(fullPath);
					fileNameWithoutExtension = azureblobService.trimCsvSuffix(fileName);
					inputFile = fullPath;
					outputFile = String.format("%s/%s%s_", output_directory, output_export_file_prefix, fileNameWithoutExtension);
					exportClassCsv(inputFile, outputFile, mapFile, encrypt, containerClient);        
				}
			}
        });
    }

/*	
    private void exportClassCsv(String inputFile, String outputFile, String mapFile, boolean encrypt, BlobContainerClient containerClient) {
		BufferedReader reader=null;
		BufferedWriter writer=null;		
		List<MapField> mapFields=null;
		int offset=0,offset_min=24;
		int index=0,num_success=0,num_error=0,num_session=0;
		boolean haveWriter = false;
		String fileNameWriter="";
        try {
            mapFields = GetMap(mapFile);
        } catch (IOException e) {
            System.out.printf("Error reading map file: %s%n", mapFile);
        }	
		try {
			haveWriter = false;
			fileNameWriter = "";
			for (offset=0; offset<offset_min; offset+=4) {
				reader = new BufferedReader(new FileReader(inputFile));
				String line = reader.readLine();
				if (line == null) {
					continue; // skip this iteration of the offset loop
				}				
				if (line != null) {
					if (line.startsWith("\uFEFF")) {
						line = line.substring(1);
					}
				}
				String[] headers = line.split(",");	
				String session_name = headers[headers.length-4-offset];
				// Sanitize session_name
				session_name = session_name.replaceAll("[^a-zA-Z0-9_-]", "_");
				//System.out.println("session name:" + session_name);
				String header_output = headers[0];
				for (int i=1; i<headers.length-offset; i++) {
					header_output = header_output + "," + headers[i];
				}
				//System.out.println(header_output);
				int row_index=0;
				index=1;			
				while ((line = reader.readLine()) != null) {
					++index;
					String[] rows = line.split(",");
					if (rows.length == headers.length-offset) {
						// Trim all rows
						rows = Arrays.stream(rows).map(String::trim).toArray(String[]::new);
						if (!validateColumn(index,mapFields,rows)) {
							++num_error;
						} else {	
							// Lookup for courseId to replace value in rows[0],rows[1],rows[2]
							String courseId = getFirstCourseIdByExternalId(rows[3]);	
							//System.out.println(rows[3] + ":" + courseId);
							if (courseId != null) {
								String escapedInput = Pattern.quote("(" + rows[3] + ")");
								String replacement = "(" + courseId + ")";
								rows[0] = rows[0].replaceAll(escapedInput, replacement);
								rows[1] = rows[1].replaceAll(escapedInput, replacement);			
								rows[3] = courseId;				
							}
							String row_output = rows[0] + ":" + mapFields.get(0).getFieldType();
							//Lookup for column:time and update format to hh:mm
							String fieldType = mapFields.get(0).getFieldType();
							if (fieldType.contains("time")) {
								rows[0]=rows[0].replaceFirst("^(\\d):", "0$1:");
							}
							for (int i=1; i<rows.length; i++) {
								//Lookup for column:time and update format to hh:mm
								fieldType = mapFields.get(i).getFieldType();
								if (fieldType.contains("time")) {
									rows[i]=rows[i].replaceFirst("^(\\d):", "0$1:");
								}	
								row_output = row_output + "," + rows[i];
							}		
							if (row_index == 0) {
								haveWriter = true;
								++num_session;
								fileNameWriter = outputFile + session_name + ".csv";
								writer = Files.newBufferedWriter(Paths.get(fileNameWriter));
								writer.write(header_output);	
								writer.newLine();							
							}
							//System.out.println(row_output);	
							writer.write(row_output);
							writer.newLine();								
							++num_success;						
							++row_index;
						}
					}
				}
				reader.close();
				if (haveWriter) {
					writer.close();
					if (encrypt) {
						String publickey=azureblobService.getVault(output_export_publickey);
						String fileNameCsv = fileNameWriter;
						String fileNamePgp = String.format("%s.pgp",fileNameWriter);
						pgpService.encryptFile (fileNameCsv, fileNamePgp, publickey);				
						azureblobService.uploadBlob(containerClient, output_directory, azureblobService.getFileName(fileNamePgp), fileNamePgp, true);			
					} else {
						azureblobService.uploadBlob(containerClient, output_directory, azureblobService.getFileName(fileNameWriter), fileNameWriter, true);				
					}
					
				}
			}
			System.out.printf("Process record(s) %d Total| %d Success| %d Error%n",num_success+num_error,num_success,num_error);
			//for (index=1; index<=num_session; index++) {
			//	System.out.printf("Create output file: %sSESSION_NAME%d.csv%n",outputFile,index);
			//}
		} catch (Exception e)  {
            System.out.printf("Error operation not completed%n");
			return;
        }
    }	
*/

/*
    private void exportClassCsv(String inputFile, String outputFile, String mapFile, boolean encrypt, BlobContainerClient containerClient) {
		BufferedReader reader=null;
		BufferedWriter writer=null;		
		List<MapField> mapFields=null;
		int offset=0,offset_min=24;
		int index=0,num_success=0,num_error=0,num_session=0;
		boolean haveWriter = false;
		String fileNameWriter="";
        try {
            mapFields = GetMap(mapFile);
        } catch (IOException e) {
            System.out.printf("Error reading map file: %s%n", mapFile);
        }	
		try {
			haveWriter = false;
			fileNameWriter = "";
			for (offset=0; offset<offset_min; offset+=4) {
				reader = new BufferedReader(new FileReader(inputFile));
				String line = reader.readLine();
				if (line == null) {
					continue; // skip this iteration of the offset loop
				}				
				if (line != null) {
					if (line.startsWith("\uFEFF")) {
						line = line.substring(1);
					}
				}
				String[] headers = line.split(",");	
				String session_name = headers[headers.length-4-offset];
				// Sanitize session_name
				session_name = session_name.replaceAll("[^a-zA-Z0-9_-]", "_");
				//System.out.println("session name:" + session_name);
				String header_output = headers[0];
				for (int i=1; i<headers.length-offset; i++) {
					header_output = header_output + "," + headers[i];
				}
				//System.out.println(header_output);
				int row_index=0;
				index=1;			
				while ((line = reader.readLine()) != null) {
					++index;
					String[] rows = line.split(",");
					if (rows.length == headers.length-offset) {
						// Trim all rows
						rows = Arrays.stream(rows).map(String::trim).toArray(String[]::new);
						if (!validateColumn(index,mapFields,rows)) {
							++num_error;
						} else {	
							// Lookup for courseId to replace value in rows[0],rows[1],rows[2]
							String courseId = getFirstCourseIdByExternalId(rows[3]);	
							//System.out.println(rows[3] + ":" + courseId);
							if (courseId != null) {
								String escapedInput = Pattern.quote("(" + rows[3] + ")");
								String replacement = "(" + courseId + ")";
								rows[0] = rows[0].replaceAll(escapedInput, replacement);
								rows[1] = rows[1].replaceAll(escapedInput, replacement);			
								rows[3] = courseId;				
							}
							String row_output = rows[0] + ":" + mapFields.get(0).getFieldType();
							//Lookup for column:time and update format to hh:mm
							String fieldType = mapFields.get(0).getFieldType();
							if (fieldType.contains("time")) {
								rows[0]=rows[0].replaceFirst("^(\\d):", "0$1:");
							}
							for (int i=1; i<rows.length; i++) {
								//Lookup for column:time and update format to hh:mm
								fieldType = mapFields.get(i).getFieldType();
								if (fieldType.contains("time")) {
									rows[i]=rows[i].replaceFirst("^(\\d):", "0$1:");
								}	
								row_output = row_output + "," + rows[i];
							}		
							if (row_index == 0) {
								haveWriter = true;
								++num_session;
								fileNameWriter = outputFile + session_name + ".csv";
								writer = Files.newBufferedWriter(Paths.get(fileNameWriter));
								writer.write(header_output);	
								writer.newLine();							
							}
							//System.out.println(row_output);	
							writer.write(row_output);
							writer.newLine();								
							++num_success;						
							++row_index;
						}
					}
				}
				reader.close();
				if (haveWriter) {
					writer.close();
					if (encrypt) {
						String publickey=azureblobService.getVault(output_export_publickey);
						String fileNameCsv = fileNameWriter;
						String fileNamePgp = String.format("%s.pgp",fileNameWriter);
						pgpService.encryptFile (fileNameCsv, fileNamePgp, publickey);				
						azureblobService.uploadBlob(containerClient, output_directory, azureblobService.getFileName(fileNamePgp), fileNamePgp, true);			
					} else {
						azureblobService.uploadBlob(containerClient, output_directory, azureblobService.getFileName(fileNameWriter), fileNameWriter, true);				
					}
					
				}
			}
			System.out.printf("Process record(s) %d Total| %d Success| %d Error%n",num_success+num_error,num_success,num_error);
			//for (index=1; index<=num_session; index++) {
			//	System.out.printf("Create output file: %sSESSION_NAME%d.csv%n",outputFile,index);
			//}
		} catch (Exception e)  {
            System.out.printf("Error operation not completed%n");
			return;
        }
    }	
*/
	
	private void exportClassCsv(String inputFile, String outputFile, String mapFile, boolean encrypt, BlobContainerClient containerClient) {
		List<MapField> mapFields = null;
		int offset = 0, offset_min = 24;
		int index = 0, num_success = 0, num_error = 0, num_session = 0;

		try {
			mapFields = GetMap(mapFile);
		} catch (IOException e) {
			System.out.printf("Error reading map file: %s%n", mapFile);
			return;
		}

		try {
			for (offset = 0; offset < offset_min; offset += 4) {
				boolean haveWriter = false;
				String fileNameWriter = "";
				int row_index = 0;

				try (BufferedReader reader = Files.newBufferedReader(Paths.get(inputFile))) {
					System.out.printf("Process input file: %s%n", inputFile);
					String line = reader.readLine();

					if (line == null) {
						System.out.printf("Input file is empty: %s%n", inputFile);
						continue; // or return, depending on your logic
					}

					if (line != null && line.startsWith("\uFEFF")) {
						line = line.substring(1); // Remove BOM
					}

					String[] headers = line.split(",");
					String session_name = headers[headers.length - 4 - offset];
					String header_output = headers[0];

					for (int i = 1; i < headers.length - offset; i++) {
						header_output = header_output + "," + headers[i];
					}

					index = 1;
					String lineRow;

					BufferedWriter writer = null;

					while ((lineRow = reader.readLine()) != null) {
						++index;
						String[] rows = lineRow.split(",");
						if (rows.length == headers.length - offset) {
							rows = Arrays.stream(rows).map(String::trim).toArray(String[]::new);
							if (!validateColumn(index, mapFields, rows)) {
								++num_error;
							} else {
								String courseId = getFirstCourseIdByExternalId(rows[3]);
								if (courseId != null) {
									String escapedInput = Pattern.quote("(" + rows[3] + ")");
									String replacement = "(" + courseId + ")";
									rows[0] = rows[0].replaceAll(escapedInput, replacement);
									rows[1] = rows[1].replaceAll(escapedInput, replacement);			
									rows[3] = courseId;
								}

								//String row_output = rows[0] + ":" + mapFields.get(0).getFieldType();

								String fieldType = mapFields.get(0).getFieldType();
								if (fieldType.contains("time")) {
									rows[0] = rows[0].replaceFirst("^(\\d):", "0$1:");
								}
								String row_output = rows[0];								

								for (int i = 1; i < rows.length; i++) {
									fieldType = mapFields.get(i).getFieldType();
									if (fieldType.contains("time")) {
										rows[i] = rows[i].replaceFirst("^(\\d):", "0$1:");
									}
									row_output = row_output + "," + rows[i];
								}

								if (row_index == 0) {
									haveWriter = true;
									++num_session;
									
									//fileNameWriter = outputFile + session_name + ".csv";
									
									session_name = session_name.replaceAll("[^a-zA-Z0-9_-]", "_");
									Path safeOutputPath = Paths.get(outputFile).resolve(session_name + ".csv").normalize();

									if (!safeOutputPath.startsWith(Paths.get(outputFile).normalize())) {
										throw new SecurityException("Path traversal attempt detected");
									}

									fileNameWriter = safeOutputPath.toString();
									
									//writer = Files.newBufferedWriter(Paths.get(fileNameWriter));
									//writer.write(header_output);
									//writer.newLine();
									
									try {
										writer = Files.newBufferedWriter(Paths.get(fileNameWriter));
										writer.write(header_output);
										writer.newLine();
									} catch (IOException e) {
										System.out.printf("Error writing header to file: %s%n", fileNameWriter);
										continue; // skip to the next offset/session
									}									
									
								}

								//writer.write(row_output);
								//writer.newLine();
								
								try {
									writer.write(row_output);
									writer.newLine();									
								} catch (IOException e) {
									System.out.printf("Error writing header to file: %s%n", fileNameWriter);
									continue; // skip to the next offset/session
								}		

								++num_success;
								++row_index;								
								
							}
						}
					}

					if (writer != null) {
						writer.close();

						if (haveWriter) {
							if (encrypt) {
								String publickey = azureblobService.getVault(output_export_publickey);
								String fileNameCsv = fileNameWriter;
								String fileNamePgp = String.format("%s.pgp", fileNameWriter);
								pgpService.encryptFile(fileNameCsv, fileNamePgp, publickey);
								azureblobService.uploadBlob(containerClient, output_directory, azureblobService.getFileName(fileNamePgp), fileNamePgp, true);
							} else {
								azureblobService.uploadBlob(containerClient, output_directory, azureblobService.getFileName(fileNameWriter), fileNameWriter, true);
							}
						}
					}

				} catch (IOException e) {
					System.out.printf("Error processing offset %d: %s%n", offset, e.getMessage());
					return;
				}
			}

			System.out.printf("Process record(s) %d Total | %d Success | %d Error%n", num_success + num_error, num_success, num_error);

		} catch (Exception e) {
			System.out.printf("Error operation not completed%n");
		}
	}

	
    public void exportRegistrationCsv(boolean encrypt) {
		String pattern=input_directory + "/" + input_export_file_pattern_registration;
		String mapFile=input_map_path_registration;  
		azureblobService.getMap(mapFile);		
        String endpoint = String.format("https://%s.blob.core.windows.net", azureblobService.getenv_account());
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .credential(azureblobService.getTokenCredential())
                .endpoint(endpoint)
                .buildClient();
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(azureblobService.getenv_container());
        Pattern regexPattern = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);		
        containerClient.listBlobsByHierarchy(input_directory + "/").forEach(blobItem -> {
            if (regexPattern.matcher(blobItem.getName()).matches()) {
				azureblobService.downloadBlob(containerClient, blobItem.getName(), input_directory);				
				String fullPath = blobItem.getName();
				String fileName = "";
				String inputFile = "";
				String outputFile = "";
				if (azureblobService.checkSuffix(fullPath,".pgp")) {
					String privatekey=azureblobService.getVault(input_export_privatekey_registration);
					fileName = azureblobService.trimPgpSuffix(azureblobService.getFileName(fullPath));				
					inputFile = azureblobService.trimPgpSuffix(fullPath);
					outputFile = String.format("%s/%s%s", output_directory, output_export_file_prefix, fileName);
					pgpService.decryptFile (fullPath, inputFile, privatekey, "");				
					exportRegistrationCsv(inputFile, outputFile, mapFile);        
				} else if (azureblobService.checkSuffix(fullPath,".csv")) {
					fileName = azureblobService.getFileName(fullPath);				
					inputFile = fullPath;
					outputFile = String.format("%s/%s%s", output_directory, output_export_file_prefix, fileName);
					exportRegistrationCsv(inputFile, outputFile, mapFile);			
				}
				if (encrypt) {
					String publickey=azureblobService.getVault(output_export_publickey);
					pgpService.encryptFile (outputFile, String.format("%s.pgp",outputFile), publickey);				
					azureblobService.uploadBlob(containerClient, output_directory, String.format("%s%s.pgp",output_export_file_prefix,fileName), String.format("%s.pgp",outputFile), true);			
				} else {
					azureblobService.uploadBlob(containerClient, output_directory, String.format("%s%s",output_export_file_prefix,fileName), outputFile, true);			
				}
			}
        });
    }
	
/*	
    private void exportRegistrationCsv (String inputFile, String outputFile, String mapFile) {
        BufferedReader reader=null;
		BufferedWriter writer=null;
		List<MapField> mapFields=null;
		try {
            reader = Files.newBufferedReader(Paths.get(inputFile));
            System.out.printf("Process input file: %s%n", inputFile);
		} catch (IOException e)  {
            System.out.printf("Error reading input file: %s%n", inputFile);
			return;
        }
        try {
            writer = Files.newBufferedWriter(Paths.get(outputFile));
		} catch (IOException e)  {
            System.out.printf("Error create output file: %s%n", outputFile);
			return;
        }
        try {
            mapFields = GetMap(mapFile);
        } catch (IOException e) {
            System.out.printf("Error reading map file: %s%n", mapFile);
        }	
		try {
            String header = reader.readLine();
            if (header != null) {
				if (header.startsWith("\uFEFF")) {
					header = header.substring(1);
				}
				// Fix: Null check for mapFields
				if (mapFields == null) {
					System.out.printf("Cannot proceed: mapFields is null due to error in map file: %s%n", mapFile);
					return;
				}				
				if (validateHeader(mapFields, header)) {
					writer.write(header);
					writer.newLine();
					String line;
					String[] headers = header.split(",");
					int index=1, num_success=0, num_error=0;
					while ((line = reader.readLine()) != null) {
						++index;
						String[] columns = line.split(",", -1); // Split by comma, keep empty fields
						if (columns.length == headers.length) {
							// Trim all columns
							columns = Arrays.stream(columns).map(String::trim).toArray(String[]::new);
							if (!validateColumn(index,mapFields,columns)) {
								++num_error;
							} else {
								// Lookup for courseId to replace value in columns[3]
								Pattern pattern = Pattern.compile("\\((.*?)\\)");
								Matcher matcher = pattern.matcher(columns[3]);
								if (matcher.find()) {
									String value = matcher.group(1); // Group 1 contains the content inside ()
									String courseId = getFirstCourseIdByExternalId(value);
									if (courseId != null) {
										// Lookup for classId in columns[3]
										columns[3] = columns[3].replaceAll("\\(.*?\\)", "(" + courseId + ")");
										writer.write(String.join(",", columns));
										writer.newLine();								
										++num_success;						
									// if NOT found courseId, error course NOT found	
									} else {
										System.out.printf("Line %d: Error in column 'OFFERING' course NOT found%n", index);
										++num_error;										
									}	
										
								} else {
									System.out.printf("Line %d: Error in column 'OFFERING' course NOT found%n", index);
									++num_error;									
								}
							}
						} else {
							System.out.printf("Line %d: Error number of column does NOT match%n", index);
							++num_error;
						}
					}
					System.out.printf("Process record(s) %d Total| %d Success| %d Error%n",num_success+num_error,num_success,num_error);
					//System.out.printf("Create output file: %s%n",outputFile);
				} else {
					System.out.println("Error file header does NOT match");
				}	
				writer.close();
				reader.close();				
            }			
		} catch (Exception e)  {
            System.out.printf("Error operation not completed%n");
			return;
        }
    }
*/
	
	private void exportRegistrationCsv(String inputFile, String outputFile, String mapFile) {
		List<MapField> mapFields = null;

		try (BufferedReader reader = Files.newBufferedReader(Paths.get(inputFile));
			 BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputFile))) {

			System.out.printf("Process input file: %s%n", inputFile);

			try {
				mapFields = GetMap(mapFile);
			} catch (IOException e) {
				System.out.printf("Error reading map file: %s%n", mapFile);
				return;
			}

			String header = reader.readLine();
			if (header != null) {
				if (header.startsWith("\uFEFF")) {
					header = header.substring(1);
				}
				if (validateHeader(mapFields, header)) {
					writer.write(header);
					writer.newLine();
					String line;
					String[] headers = header.split(",");
					int index = 1, num_success = 0, num_error = 0;

					while ((line = reader.readLine()) != null) {
						++index;
						String[] columns = line.split(",", -1);
						if (columns.length == headers.length) {
							columns = Arrays.stream(columns).map(String::trim).toArray(String[]::new);
							if (!validateColumn(index, mapFields, columns)) {
								++num_error;
							} else {
								Pattern pattern = Pattern.compile("\\((.*?)\\)");
								Matcher matcher = pattern.matcher(columns[3]);
								if (matcher.find()) {
									String value = matcher.group(1);
									String courseId = getFirstCourseIdByExternalId(value);
									if (courseId != null) {
										columns[3] = columns[3].replaceAll("\\(.*?\\)", "(" + courseId + ")");
										writer.write(String.join(",", columns));
										writer.newLine();
										++num_success;
									} else {
										System.out.printf("Line %d: Error in column 'OFFERING' course NOT found%n", index);
										++num_error;
									}
								} else {
									System.out.printf("Line %d: Error in column 'OFFERING' course NOT found%n", index);
									++num_error;
								}
							}
						} else {
							System.out.printf("Line %d: Error number of column does NOT match%n", index);
							++num_error;
						}
					}
					System.out.printf("Process record(s) %d Total| %d Success| %d Error%n",
							num_success + num_error, num_success, num_error);
				} else {
					System.out.println("Error file header does NOT match");
				}
			}

		} catch (IOException e) {
			System.out.printf("Error reading or writing file: %s%n", e.getMessage());
		} catch (Exception e) {
			System.out.printf("Error operation not completed%n");
		}
	}	
	

    public void exportTranscriptCsv(boolean encrypt) {
		String pattern=input_directory + "/" + input_export_file_pattern_transcript;
		String mapFile=input_map_path_transcript;        
		azureblobService.getMap(mapFile);
        String endpoint = String.format("https://%s.blob.core.windows.net", azureblobService.getenv_account());
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .credential(azureblobService.getTokenCredential())
                .endpoint(endpoint)
                .buildClient();
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(azureblobService.getenv_container());
        Pattern regexPattern = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);		
        containerClient.listBlobsByHierarchy(input_directory + "/").forEach(blobItem -> {
            if (regexPattern.matcher(blobItem.getName()).matches()) {
				azureblobService.downloadBlob(containerClient, blobItem.getName(), input_directory);				
				String fullPath = blobItem.getName();
				String fileName = "";
				String inputFile = "";
				String outputFile = "";
				if (azureblobService.checkSuffix(fullPath,".pgp")) {
					String privatekey=azureblobService.getVault(input_export_privatekey_transcript);
					fileName = azureblobService.trimPgpSuffix(azureblobService.getFileName(fullPath));				
					inputFile = azureblobService.trimPgpSuffix(fullPath);
					outputFile = String.format("%s/%s%s", output_directory, output_export_file_prefix, fileName);
					pgpService.decryptFile (fullPath, inputFile, privatekey, "");				
					exportTranscriptCsv(inputFile, outputFile, mapFile);        
				} else if (azureblobService.checkSuffix(fullPath,".csv")) {
					fileName = azureblobService.getFileName(fullPath);				
					inputFile = fullPath;
					outputFile = String.format("%s/%s%s", output_directory, output_export_file_prefix, fileName);
					exportTranscriptCsv(inputFile, outputFile, mapFile);			
				}
				if (encrypt) {
					String publickey=azureblobService.getVault(output_export_publickey);
					pgpService.encryptFile (outputFile, String.format("%s.pgp",outputFile), publickey);				
					azureblobService.uploadBlob(containerClient, output_directory, String.format("%s%s.pgp",output_export_file_prefix,fileName), String.format("%s.pgp",outputFile), true);			
				} else {
					azureblobService.uploadBlob(containerClient, output_directory, String.format("%s%s",output_export_file_prefix,fileName), outputFile, true);			
				}
			}
        });
    }
	
/*	
    private void exportTranscriptCsv (String inputFile, String outputFile, String mapFile) {
        BufferedReader reader=null;
		BufferedWriter writer=null;
		List<MapField> mapFields=null;
		try {
            reader = Files.newBufferedReader(Paths.get(inputFile));
            System.out.printf("Process input file: %s%n", inputFile);
		} catch (IOException e)  {
            System.out.printf("Error reading input file: %s%n", inputFile);
			return;
        }
        try {
            writer = Files.newBufferedWriter(Paths.get(outputFile));
		} catch (IOException e)  {
            System.out.printf("Error create output file: %s%n", outputFile);
			return;
        }
        try {
            mapFields = GetMap(mapFile);
        } catch (IOException e) {
            System.out.printf("Error reading map file: %s%n", mapFile);
        }	
		try {
            String header = reader.readLine();
            if (header != null) {
				if (header.startsWith("\uFEFF")) {
					header = header.substring(1);
				}
				// Fix: Null check for mapFields
				if (mapFields == null) {
					System.out.printf("Cannot proceed: mapFields is null due to error in map file: %s%n", mapFile);
					return;
				}				
				if (validateHeader(mapFields, header)) {
					writer.write(header);
					writer.newLine();
					String line;
					String[] headers = header.split(",");
					int index=1, num_success=0, num_error=0;
					while ((line = reader.readLine()) != null) {
						++index;
						String[] columns = line.split(",", -1); // Split by comma, keep empty fields
						if (columns.length == headers.length) {
							// Trim all columns
							columns = Arrays.stream(columns).map(String::trim).toArray(String[]::new);
							if (!validateColumn(index,mapFields,columns)) {
								++num_error;
							} else {
								// Lookup for courseId to replace value in columns[1]
								Pattern pattern = Pattern.compile("\\((.*?)\\)");
								Matcher matcher = pattern.matcher(columns[1]);
								if (matcher.find()) {
									String value = matcher.group(1); // Group 1 contains the content inside ()
									String courseId = getFirstCourseIdByExternalId(value);
									if (courseId != null) {
										// Lookup for classId in columns[1]
										columns[1] = columns[1].replaceAll("\\(.*?\\)", "(" + courseId + ")");
										writer.write(String.join(",", columns));
										writer.newLine();								
										++num_success;								
									// if NOT found courseId, error course NOT found	
									} else {
										System.out.printf("Line %d: Error in column 'OFFERING' course NOT found%n", index);
										++num_error;										
									}	
										
								} else {
									System.out.printf("Line %d: Error in column 'OFFERING' course NOT found%n", index);
									++num_error;									
								}								
								
							}
						} else {
							System.out.printf("Line %d: Error number of column does NOT match%n", index);
							++num_error;
						}
					}
					System.out.printf("Process record(s) %d Total| %d Success| %d Error%n",num_success+num_error,num_success,num_error);
					//System.out.printf("Create output file: %s%n",outputFile);
				} else {
					System.out.println("Error file header does NOT match");
				}	
				writer.close();
				reader.close();				
            }			
		} catch (Exception e)  {
            System.out.printf("Error operation not completed%n");
			return;
        }
    }	
*/

	private void exportTranscriptCsv(String inputFile, String outputFile, String mapFile) {
		List<MapField> mapFields = null;

		try {
			mapFields = GetMap(mapFile);
		} catch (IOException e) {
			System.out.printf("Error reading map file: %s%n", mapFile);
			return;
		}

		try (
			BufferedReader reader = Files.newBufferedReader(Paths.get(inputFile));
			BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputFile));
		) {
			System.out.printf("Process input file: %s%n", inputFile);

			String header = reader.readLine();
			if (header != null) {
				if (header.startsWith("\uFEFF")) {
					header = header.substring(1);
				}

				if (validateHeader(mapFields, header)) {
					writer.write(header);
					writer.newLine();
					String line;
					String[] headers = header.split(",");
					int index = 1, num_success = 0, num_error = 0;

					while ((line = reader.readLine()) != null) {
						++index;
						String[] columns = line.split(",", -1);
						if (columns.length == headers.length) {
							columns = Arrays.stream(columns).map(String::trim).toArray(String[]::new);
							if (!validateColumn(index, mapFields, columns)) {
								++num_error;
							} else {
								Pattern pattern = Pattern.compile("\\((.*?)\\)");
								Matcher matcher = pattern.matcher(columns[1]);
								if (matcher.find()) {
									String value = matcher.group(1);
									String courseId = getFirstCourseIdByExternalId(value);
									if (courseId != null) {
										columns[1] = columns[1].replaceAll("\\(.*?\\)", "(" + courseId + ")");
										// Optionally format time fields
										for (int i = 0; i < columns.length; i++) {
											String fieldType = mapFields.get(i).getFieldType();
											if (fieldType.contains("time")) {
												columns[i] = columns[i].replaceFirst("^(\\d):", "0$1:");
											}
										}
										writer.write(String.join(",", columns));
										writer.newLine();
										++num_success;
									} else {
										System.out.printf("Line %d: Error in column 'OFFERING' course NOT found%n", index);
										++num_error;
									}
								} else {
									System.out.printf("Line %d: Error in column 'OFFERING' course NOT found%n", index);
									++num_error;
								}
							}
						} else {
							System.out.printf("Line %d: Error number of column does NOT match%n", index);
							++num_error;
						}
					}
					System.out.printf("Process record(s) %d Total| %d Success| %d Error%n", num_success + num_error, num_success, num_error);
				} else {
					System.out.println("Error file header does NOT match");
				}
			}
		} catch (IOException e) {
			System.out.printf("Error processing files: %s%n", e.getMessage());
		}
	}

	
    private String getFirstCourseIdByExternalId(String courseExternalCourseId) {
        List<OfferingEntity> entities = offeringRepository.findByCourseExternalCourseId(courseExternalCourseId);
        // Return the courseId of the first record, if available
        return entities != null && !entities.isEmpty() ? entities.get(0).getCourseId() : null;
    }	
	
    private boolean isClassAndUserNameExists(String classId, String userName) {
        boolean result = registrationRepository.existsByClassAndUserName(classId, userName);
		return result;
    }	

    private static List<MapField> GetMap(String mapFilePath) throws IOException {
        List<MapField> mapFields = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(mapFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.contains("=")) {
                    String[] parts = line.split("=");
                    String fieldName = parts[0].trim();
                    String fieldTypeDetails = parts[1].trim();
                    boolean isMandatory = fieldTypeDetails.contains(":mandatory");
                    String fieldType = fieldTypeDetails.replace(":mandatory", "").trim();
                    MapField mapField = new MapField(fieldName, fieldType, isMandatory);
                    mapFields.add(mapField);
                }
            }
        }
        return mapFields;
    }

    private static boolean validateField(String fieldName,String fieldType,boolean isMandatory,String value) {
		try {
			if (isMandatory && (value == null || value.trim().isEmpty())) {
				return false;
			}
			if (fieldType.contains("string")) {
                // Extract max length from string definition (e.g., string(25))
                Pattern pattern = Pattern.compile("string\\((\\d+)\\)");
                Matcher matcher = pattern.matcher(fieldType);
                if (matcher.find()) {
                    int maxLength = Integer.parseInt(matcher.group(1));
					return value.length() <= maxLength; // Check string length
                }
            } else if (fieldType.contains("date")) {
                // Extract date format from date definition (e.g., date(yyyy-MM-dd))
                if (value.equals("")) {
					return true;
				}
				Pattern pattern = Pattern.compile("date\\(([^)]+)\\)");
                Matcher matcher = pattern.matcher(fieldType);
                if (matcher.find()) {
                    String dateFormat = matcher.group(1);
                    SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
                    sdf.setLenient(false); // Disable lenient parsing to catch invalid dates
                    try {
                        sdf.parse(value); // Try parsing the date
                        return true;
                    } catch (ParseException e) {
                        return false; // Invalid date format
                    }
                }
            } else if (fieldType.contains("boolean")) {
                return value.toLowerCase().equals("true") || value.toLowerCase().equals("false") || value.equals("");
            } else if (fieldType.contains("integer")) {
                if (value.equals("")) {
					return true;
				}				
                try {
                    Integer.parseInt(value);
                    return true;
                } catch (NumberFormatException e) {
                    return false; // Invalid integer
                }
            } else if (fieldType.contains("float")) {
                if (value.equals("")) {
					return true;
				}				
                try {
                    Float.parseFloat(value);
                    return true;
                } catch (NumberFormatException e) {
                    return false; // Invalid float
                }
            } else if (fieldType.contains("time")) {
				if (value.equals("")) {
					return true;
				}
				String timeRegex = "^([0-9]|0[0-9]|1[0-9]|2[0-3]):[0-5][0-9]$"; // Matches 0:00 to 23:59
				return value.matches(timeRegex);
            }
        } catch (Exception e) {
            return false; // If any validation fails, return false
        }
        return false;
    }

	private static boolean validateHeader(List<MapField> mapFields, String header) {
		String headerMap = mapFields.stream()
            .map(MapField::getFieldName) // Extract the fieldName
            .collect(Collectors.joining(",")); // Join them with commas	
		return headerMap.equals(header);
	}
	
    private static boolean validateColumn(int Line, List<MapField> mapFields, String[] value) {
  	    int index=0;
		boolean hasError=false;
		for (MapField field : mapFields) {
			String fieldName = field.getFieldName();
			String fieldType = field.getFieldType();
			boolean isMandatory = field.isMandatory();
			String mandatory = "";
			if (isMandatory) {
				mandatory = ":mandatory";
			}
            if (!validateField(fieldName,fieldType,isMandatory,value[index])) {
                System.out.printf("Line %d: Error column '%s' type '%s%s' not match%n", Line, fieldName, fieldType,mandatory);
				hasError=true;
			}			
			++index;
			if (index >= value.length) {
				break;
			}
        }	
		return !hasError;
	}	
	
    public void deleteAllFilesInOutput() {
        Path directory = Paths.get(output_directory);
        try {
            if (Files.exists(directory) && Files.isDirectory(directory)) {
                Files.list(directory).forEach(file -> {
                    try {
                        Files.delete(file);
                        //System.out.println("Deleted: " + file);
                    } catch (IOException e) {
                        //System.err.println("Failed to delete: " + file + " - " + e.getMessage());
                    }
                });
                System.out.printf("All files in directory '%s' have been deleted%n", directory);
            } else {
                System.out.printf("Directory '%s' does not exist or is not a directory%n", directory);
            }
        } catch (IOException e) {
            System.err.printf("Error accessing directory '%s'%n", directory);
        }
    }	
	
    public void deleteAllFilesInLog() {
        Path directory = Paths.get(log_directory);
        try {
            if (Files.exists(directory) && Files.isDirectory(directory)) {
                Files.list(directory).forEach(file -> {
                    try {
                        Files.delete(file);
                        //System.out.println("Deleted: " + file);
                    } catch (IOException e) {
                        //System.err.println("Failed to delete: " + file + " - " + e.getMessage());
                    }
                });
                System.out.printf("All files in directory '%s' have been deleted%n", directory);
            } else {
                System.out.printf("Directory '%s' does not exist or is not a directory%n", directory);
            }
        } catch (IOException e) {
            System.err.printf("Error accessing directory '%s'%n", directory);
        }
    }		
	
}

// Helper class to store map field information
class MapField {
    private String fieldName;
    private String fieldType;
    private boolean isMandatory;

    public MapField(String fieldName, String fieldType, boolean isMandatory) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.isMandatory = isMandatory;
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getFieldType() {
        return fieldType;
    }

    public boolean isMandatory() {
        return isMandatory;
    }

    @Override
    public String toString() {
        return "MapField{" +
                "fieldName='" + fieldName + '\'' +
                ", fieldType='" + fieldType + '\'' +
                ", isMandatory=" + isMandatory +
                '}';
    }
}
