package com.vlink.iad.service;

import com.vlink.iad.entity.OfferingEntity;
import com.vlink.iad.entity.RegistrationEntity;
import com.vlink.iad.repository.OfferingRepository;
import com.vlink.iad.repository.RegistrationRepository;
import com.vlink.iad.service.PgpService;
import com.vlink.iad.service.AzureBlobService;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;

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
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

@Service
public class CsvImportService {

    @Value("${input.directory}")
    private String input_directory;
    @Value("${input.import.file.pattern.offering}")
    private String input_import_file_pattern_offering;	
    @Value("${input.import.pvk.offering}")
    private String input_import_privatekey_offering;		
    @Value("${input.import.file.pattern.registration}")
    private String input_import_file_pattern_registration;	
    @Value("${input.import.pvk.registration}")
    private String input_import_privatekey_registration;
	
    private final OfferingRepository offeringRepository;	
    private final RegistrationRepository registrationRepository;
    private final PgpService pgpService;
	private final AzureBlobService azureblobService;
	
    @Autowired
    public CsvImportService(
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
	
    public void viewOffering() {
        List<OfferingEntity> offerings = offeringRepository.findAll();
        if (offerings.isEmpty()) {
            System.out.println("No offerings available.");
            return;
        }
        System.out.printf("%-25s %-50s %-25s%n", "Course ID", "Course External Course ID", "Class ID");
        System.out.println("=".repeat(100));
        for (OfferingEntity offering : offerings) {
            System.out.printf("%-25s %-50s %-25s%n", 
                offering.getCourseId(), 
                offering.getCourseExternalCourseId(), 
                offering.getClassId()
            );
        }
    }
	
    public void deleteAllOfferings() {
        offeringRepository.deleteAll();
        System.out.println("All offerings have been deleted.");
    }		
	
    private boolean isOfferingExists(String courseId, String courseExternalCourseId, String classId) {
        boolean result = offeringRepository.existsByOffering(courseId, courseExternalCourseId, classId);
		return result;
    }
	
    public void importOfferingEntity() {
		String pattern=input_directory + "/" + input_import_file_pattern_offering;
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
				String inputFile = blobItem.getName();
				if (azureblobService.checkSuffix(inputFile,".pgp")) {
					String key=azureblobService.getVault(input_import_privatekey_offering);
					String inputFileCsv = azureblobService.trimPgpSuffix(inputFile);		
					pgpService.decryptFile (inputFile, inputFileCsv, key, "");				
					importOfferingEntity(inputFileCsv); 					
				} else if (azureblobService.checkSuffix(inputFile,".csv")) {
					importOfferingEntity(inputFile); 					
				}	
			}
        });
    }	
	
    private void importOfferingEntity(String csvFilePath) {
        try (FileReader fileReader = new FileReader(csvFilePath);
             CSVReader reader = new CSVReaderBuilder(fileReader)
                     .withCSVParser(new CSVParserBuilder()
                             .withSeparator('|')
                             .withQuoteChar('"')
                             .build())
                     .build()) {
            String[] nextLine;
			int index=0,length=0;
			int num_insert=0,num_exist=0,num_error=0;
			System.out.printf("Process input file: %s%n", csvFilePath);

            while ((nextLine = reader.readNext()) != null) {
				++index;
				// Skip blank row
				if (nextLine.length == 1) {
					continue;
				}
				// Skip header
				if ("Course ID".equalsIgnoreCase(nextLine[0])) {
					length = nextLine.length;
					continue;
				}	
				if (nextLine.length == length) {
					if (!isOfferingExists(nextLine[0],nextLine[1],nextLine[2])) {
						if (isValidCourseId(nextLine[0],nextLine[1])) {
							if (isValidCourseExternalCourseId(nextLine[0],nextLine[1])) {
								++num_insert;
								OfferingEntity offeringEntity = new OfferingEntity();
								offeringEntity.setCourseId(nextLine[0]);
								offeringEntity.setCourseExternalCourseId(nextLine[1]);
								offeringEntity.setClassId(nextLine[2]);
								offeringRepository.save(offeringEntity);
							} else {
								++num_error;
								System.out.printf("Line %d: Error column 'Course External Course ID':'%s' is conflicted with existing record%n", index,nextLine[1]);
							}
						} else {
							++num_error;
							System.out.printf("Line %d: Error column 'Course ID':'%s' is conflicted with existing record%n",index,nextLine[0]);
						}
					} else {
						++num_exist;
					}
				} else {
					++num_error;
					System.out.printf("Line %d: Error number of column does NOT match%n", index);
				}
            }
			System.out.printf("Process record(s) %d Total| %d Insert| %d Exist| %d Error%n",num_insert+num_exist+num_error,num_insert,num_exist,num_error);
        } catch (IOException e) {
            System.out.printf("Error reading input file: %s%n", csvFilePath);
        } catch (CsvValidationException e) {
            System.out.println("Error invalid CSV file%n");
        }		
    }	
	
    public void viewRegistration() {
        List<RegistrationEntity> registrations = registrationRepository.findAll();
        if (registrations.isEmpty()) {
            System.out.println("No registrations available.");
            return;
        }
        System.out.printf("%-25s %-25s %-25s%n", "Class ID", "Username", "Registration Date");
        System.out.println("=".repeat(75));
        for (RegistrationEntity registration : registrations) {
            System.out.printf("%-25s %-25s %-25s%n", 
                registration.getClassId(), 
                registration.getUserName(), 
                registration.getRegistrationDate()
            );
        }
    }
	
    public void deleteAllRegistrations() {
        registrationRepository.deleteAll();
        System.out.println("All registrations have been deleted.");
    }
	
    private boolean isRegistrationExists(String classId, String userName, String registrationDate) {
        boolean result = registrationRepository.existsByRegistration(classId, userName, registrationDate);
		return result;
    }	
	
    public void importRegistrationEntity() {
		String pattern=input_directory + "/" + input_import_file_pattern_registration;
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
				String inputFile = blobItem.getName();
				if (azureblobService.checkSuffix(inputFile,".pgp")) {
					String key=azureblobService.getVault(input_import_privatekey_registration);
					String inputFileCsv = azureblobService.trimPgpSuffix(inputFile);		
					pgpService.decryptFile (inputFile, inputFileCsv, key, "");				
					importRegistrationEntity(inputFileCsv); 					
				} else if (azureblobService.checkSuffix(inputFile,".csv")) {
					importRegistrationEntity(inputFile); 					
				} 
			}
        });
    }		
	
    private void importRegistrationEntity(String csvFilePath) {
        try (FileReader fileReader = new FileReader(csvFilePath);
             CSVReader reader = new CSVReaderBuilder(fileReader)
                     .withCSVParser(new CSVParserBuilder()
                             .withSeparator('|')
                             .withQuoteChar('"')
                             .build())
                     .build()) {
            String[] nextLine;
			int index=0,length=0;
			int num_insert=0,num_exist=0,num_error=0;
			System.out.printf("Process input file: %s%n", csvFilePath);
			
            while ((nextLine = reader.readNext()) != null) {
				++index;
				// Skip blank row
				if (nextLine.length == 1) {
					continue;
				}
				// Skip header
				if ("Class ID".equalsIgnoreCase(nextLine[0])) {
					length = nextLine.length;
					continue;
				}
				if (nextLine.length == length) {
					if (!isRegistrationExists(nextLine[0],nextLine[1],nextLine[2])) {
						++num_insert;
						RegistrationEntity registrationEntity = new RegistrationEntity();
						registrationEntity.setClassId(nextLine[0]);
						registrationEntity.setUserName(nextLine[1]);
						registrationEntity.setRegistrationDate(nextLine[2]);
						registrationRepository.save(registrationEntity);
					} else {
						++num_exist;
					}
				} else {
					++num_error;
					System.out.printf("Line %d: Error number of column does NOT match%n", index);
				}				
            }
			System.out.printf("Process record(s) %d Total| %d Insert| %d Exist| %d Error%n",num_insert+num_exist+num_error,num_insert,num_exist,num_error);
        } catch (IOException e) {
            System.out.printf("Error reading input file: %s%n", csvFilePath);
        } catch (CsvValidationException e) {
            System.out.println("Error invalid CSV file%n");
        }
    }	

    private boolean isValidCourseId(String courseId, String courseExternalCourseId) {
		List<String> externalIds = offeringRepository.getAllCourseExternalCourseIdsByCourseId(courseId);
		if (externalIds.isEmpty()) {
			return true; 
		}
		return externalIds.contains(courseExternalCourseId);
	}

    private boolean isValidCourseExternalCourseId(String courseId, String courseExternalCourseId) {
		List<String> Ids = offeringRepository.getAllCourseIdsByCourseExternalCourseId(courseExternalCourseId);
		if (Ids.isEmpty()) {
			return true; 
		}
		return Ids.contains(courseId);
	}

}
