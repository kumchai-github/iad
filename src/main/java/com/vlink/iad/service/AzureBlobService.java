package com.vlink.iad.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.core.credential.TokenCredential;
import com.azure.storage.blob.*;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;
import com.azure.security.keyvault.secrets.models.KeyVaultSecret;

import java.io.*;
import java.nio.file.*;
import java.lang.reflect.Method;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.regex.*;

@Service
public class AzureBlobService {

	@Value("${data.directory}")
	private String data_directory;
	@Value("${data.filename}")
	private String data_filename;	
	@Value("${data.filename.init}")
	private String data_filename_init;	
	@Value("${log.directory}")
	private String log_directory;
	@Value("${log.filename}")
	private String log_filename;
	@Value("${log.pbk}")
	private String log_publickey;		
	@Value("${input.map.directory}")
	private String input_map_directory;

    private final String azureblob_account;
    private final String azureblob_container;
    private final String azurevault_account;
    private final String azureumi_clientid;	
    private final PgpService pgpService;
    private final FileService fileService;
	
    @Autowired
    public AzureBlobService(
		PgpService pgpService,
		FileService fileService
		) 
	{
        this.pgpService = pgpService;
        this.fileService = fileService;	
		this.azureblob_account = System.getenv("AZUREBLOB_ACCOUNT");
		this.azureblob_container = System.getenv("AZUREBLOB_CONTAINER");
		this.azurevault_account = System.getenv("AZUREVAULT_ACCOUNT");
		this.azureumi_clientid = System.getenv("AZUREUMI_CLIENTID");
    }	
	
	public String getenv_account() {
		return azureblob_account;
	}
	
	public String getenv_container() {
		return azureblob_container;
	}
	
	public TokenCredential getTokenCredential() {
		TokenCredential token;
        if (azureumi_clientid != null) {
            token = new DefaultAzureCredentialBuilder().managedIdentityClientId(azureumi_clientid).build();
        } else {
            token = new DefaultAzureCredentialBuilder().build();
        }		
		return token;
	}
	
    public void getData() {
        //TokenCredential tokenCredential = new DefaultAzureCredentialBuilder().build();
        String endpoint = String.format("https://%s.blob.core.windows.net", azureblob_account);
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .credential(getTokenCredential())
                .endpoint(endpoint)
                .buildClient();
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(azureblob_container);
        downloadBlob(containerClient, data_directory + "/" + data_filename, data_directory);		
	}
	
    public void putData(boolean init) {
        //TokenCredential tokenCredential = new DefaultAzureCredentialBuilder().build();
        String endpoint = String.format("https://%s.blob.core.windows.net", azureblob_account);
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .credential(getTokenCredential())
                .endpoint(endpoint)
                .buildClient();
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(azureblob_container);
		if (init) {
			int result = uploadBlob(containerClient, data_directory, data_filename, data_directory + "/" + data_filename_init, false);    
			if (result == 0) {
				System.out.printf("Success: Initialize data%n");
			} else if (result == -1) {
				System.out.printf("Error: Local data file does not exist%n");
			} else if (result == -2) {
				System.out.printf("Error: Blob data file existed%n");
			} else if (result == -3) {
				System.out.printf("Error: Blob data operation failure%n");
			} else {
				System.out.printf("Error: Unknown code%n");
			}
		} else {
			uploadBlob(containerClient, data_directory, data_filename, data_directory + "/" + data_filename, true);    
		}
	}

    public void putMap() {
        //TokenCredential tokenCredential = new DefaultAzureCredentialBuilder().build();
        String endpoint = String.format("https://%s.blob.core.windows.net", azureblob_account);
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .credential(getTokenCredential())
                .endpoint(endpoint)
                .buildClient();
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(azureblob_container);
		try {
			List<String> filteredFiles = fileService.getFilteredFiles(input_map_directory, ".*\\.map");
			if (!filteredFiles.isEmpty()) {
				for (String file : filteredFiles) {
					String fileName = new File(file).getName();
					uploadBlob(containerClient, input_map_directory, fileName, String.format("%s/%s", input_map_directory, fileName), true);    
				}
				System.out.printf("Success: Initialize map files%n");
			} else {
				System.out.printf("Map file not found in directory: %s%n",input_map_directory);
			}	
		} catch (IOException e)  {
            System.out.printf("Error reading directory: %s%n",input_map_directory);
			return;
        }			
	}
	
	public String trimPgpSuffix(String input) {
		if (input != null && input.toLowerCase().endsWith(".pgp")) {
			return input.substring(0, input.length() - 4);
		}
		return input;
	}
	
	public boolean checkSuffix(String input, String suffix) {
		if (input != null && input.toLowerCase().endsWith(suffix.toLowerCase())) {
			return true;
		}
		return false;
	}	
	
	public String trimCsvSuffix(String input) {
		if (input != null && input.toLowerCase().endsWith(".csv")) {
			return input.substring(0, input.length() - 4);
		}
		return input;
	}	
	
	public String getDirectory(String blobName) {
		int lastSlashIndex = blobName.lastIndexOf('/');
		return lastSlashIndex != -1 ? blobName.substring(0, lastSlashIndex) : "";
	}

	public String getFileName(String blobName) {
		int lastSlashIndex = blobName.lastIndexOf('/');
		return lastSlashIndex != -1 ? blobName.substring(lastSlashIndex + 1) : blobName;
	}	
	
    public String getVault(String secretName) {
		String endpoint = String.format("https://%s.vault.azure.net/", azurevault_account);
        SecretClient secretClient = new SecretClientBuilder()
                .vaultUrl(endpoint)
                .credential(getTokenCredential())
                .buildClient();
        KeyVaultSecret secret = secretClient.getSecret(secretName);
		return secret.getValue();
	}	
	
    public void getLog() {
        //TokenCredential tokenCredential = new DefaultAzureCredentialBuilder().build();
        String endpoint = String.format("https://%s.blob.core.windows.net", azureblob_account);
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .credential(getTokenCredential())
                .endpoint(endpoint)
                .buildClient();
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(azureblob_container);
        downloadBlob(containerClient, log_directory + "/" + log_filename, log_directory);		
	}

    public void putLog(boolean datestamp, boolean encrypt) {
        //TokenCredential tokenCredential = new DefaultAzureCredentialBuilder().build();
        String endpoint = String.format("https://%s.blob.core.windows.net", azureblob_account);
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .credential(getTokenCredential())
                .endpoint(endpoint)
                .buildClient();
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(azureblob_container);
		String newlog_filename = log_filename;
		if (datestamp) {
			int dotIndex = log_filename.lastIndexOf(".");
			String baseName = log_filename.substring(0, dotIndex);
			String extension = log_filename.substring(dotIndex);
			String currentDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
			newlog_filename = baseName + "_" + currentDate + extension;		
		} else {
			newlog_filename = log_filename;
		}
		if (encrypt) {
			String outputFile = log_directory + "/" + log_filename;
			String publickey = getVault(log_publickey);
			pgpService.encryptFile (outputFile, String.format("%s.pgp",outputFile), publickey);				
			uploadBlob(containerClient, log_directory, String.format("%s.pgp",newlog_filename), String.format("%s.pgp",outputFile), true);    
		} else {
			uploadBlob(containerClient, log_directory, newlog_filename, log_directory + "/" + log_filename, true);    
		}
	}	
	
    public void getMap(String mapFile) {
		String map_directory = getDirectory(mapFile);
		String map_filename = getFileName(mapFile);
        //TokenCredential tokenCredential = new DefaultAzureCredentialBuilder().build();
        String endpoint = String.format("https://%s.blob.core.windows.net", azureblob_account);
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .credential(getTokenCredential())
                .endpoint(endpoint)
                .buildClient();
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(azureblob_container);
        downloadBlob(containerClient, map_directory + "/" + map_filename, map_directory);		
	}	

	/*
	public void downloadBlob(BlobContainerClient containerClient, String blobName, String localDirectory, boolean delete) {
        BlobClient blobClient = containerClient.getBlobClient(blobName);
        File localDir = new File(localDirectory);
        if (!localDir.exists() && !localDir.mkdirs()) {
            System.err.println("Failed to create local directory: " + localDirectory);
            return;
        }
        File localFile = new File(localDir, blobName.substring(blobName.lastIndexOf('/') + 1));
        try (FileOutputStream fileOutputStream = new FileOutputStream(localFile)) {
            blobClient.download(fileOutputStream);
            //System.out.println("Downloaded: " + blobName + " to " + localFile.getAbsolutePath());
			if (delete) {
				blobClient.delete();
			}
		} catch (IOException e) {
            System.err.println("Failed to download blob: " + blobName + ". Error: " + e.getMessage());
			return;
		}
    }
	*/
	
	public void downloadBlob(BlobContainerClient containerClient, String blobName, String localDirectory) {
		BlobClient blobClient = containerClient.getBlobClient(blobName);
		File localDir = new File(localDirectory);
		if (!localDir.exists() && !localDir.mkdirs()) {
			System.err.println("Failed to create local directory: " + localDirectory);
			return;
		}
		File localFile = new File(localDir, blobName.substring(blobName.lastIndexOf('/') + 1));
		try (FileOutputStream fileOutputStream = new FileOutputStream(localFile)) {
			blobClient.download(fileOutputStream);
			//System.out.println("Downloaded: " + blobName + " to " + localFile.getAbsolutePath());
			/*
			if (archive) {
				String archiveBlobDirectory = input_archived_directory;
				BlobClient archiveBlobClient = containerClient.getBlobClient(archiveBlobDirectory + "/" + blobName.substring(blobName.lastIndexOf('/') + 1));
				try (InputStream inputStream = new FileInputStream(localFile)) {
					archiveBlobClient.upload(inputStream, localFile.length(), true);
					//System.out.println("Archived: " + blobName + " to " + archiveBlobDirectory);
					blobClient.delete();
				} catch (IOException e) {
					System.err.println("Failed to archive blob: " + blobName + ". Error: " + e.getMessage());
				}
			}
			*/
		} catch (IOException e) {
			System.err.println("Failed to download blob: " + blobName + ". Error: " + e.getMessage());
		}
	}	

	public int uploadBlob(BlobContainerClient containerClient, String blobDirectory, String blobFileName, String localFilePath, boolean replace) {
		String blobName = blobDirectory + "/" + blobFileName;
		BlockBlobClient blockBlobClient = containerClient.getBlobClient(blobName).getBlockBlobClient();
		Path filePath = Paths.get(localFilePath);
		if (!Files.exists(filePath)) {
			return -1;
		}
		try {		
			if (!replace && blockBlobClient.exists()) {
				return -2;
			}		
			try (BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(localFilePath))) {
				long fileSize = Files.size(filePath); // Get the size of the file
				blockBlobClient.upload(bufferedInputStream, fileSize, true); // Upload with overwrite enabled
			}
		} catch (IOException e) {
			return -3;
		}		
		return 0;
	}	
	
	public void deleteAllFilesInDirectory(String directoryName) {
		//TokenCredential tokenCredential = new DefaultAzureCredentialBuilder().build();
        String endpoint = String.format("https://%s.blob.core.windows.net", azureblob_account);
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .credential(getTokenCredential())
                .endpoint(endpoint)
                .buildClient();
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(azureblob_container);
		if (directoryName == null || directoryName.isEmpty()) {
			System.err.println("Directory name must not be null or empty.");
			return;
		}
		if (!directoryName.endsWith("/")) {
			directoryName += "/";
		}
		try {
			Iterable<BlobItem> blobs = containerClient.listBlobsByHierarchy(directoryName);
			for (BlobItem blobItem : blobs) {
				if (blobItem.isPrefix()) {
					// Skip directories, process only files
					continue;
				}
				String blobName = blobItem.getName();
				//System.out.println("Deleting blob: " + blobName);
				// Get the BlobClient and delete the blob
				containerClient.getBlobClient(blobName).delete();
			}
			//System.out.println("All files in directory '" + directoryName + "' have been deleted.");
		} catch (Exception e) {
			System.err.println("Failed to delete files in directory '" + directoryName + "'. Error: " + e.getMessage());
		}
	}
	
	public void deleteFilesInDirectory(String directoryName, int retention_day) {
		//TokenCredential tokenCredential = new DefaultAzureCredentialBuilder().build();
        String endpoint = String.format("https://%s.blob.core.windows.net", azureblob_account);
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .credential(getTokenCredential())
                .endpoint(endpoint)
                .buildClient();
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(azureblob_container);
		if (directoryName == null || directoryName.isEmpty()) {
			System.err.println("Directory name must not be null or empty.");
			return;
		}
		if (!directoryName.endsWith("/")) {
			directoryName += "/";
		}
		try {
			Iterable<BlobItem> blobs = containerClient.listBlobsByHierarchy(directoryName);
			for (BlobItem blobItem : blobs) {
				if (blobItem.isPrefix()) {
					// Skip directories, process only files
					continue;
				}
				String blobName = blobItem.getName();
				//System.out.println("Found blob: " + blobName);
				Pattern pattern = Pattern.compile("(\\d{8})\\.");
				Matcher matcher = pattern.matcher(blobName);
				if (matcher.find()) {
					
					/*
					String dateStr = matcher.group(1);
					//System.out.println("Extracted Date: " + dateStr); // Output: 20240310
					DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
					LocalDate fileDate = LocalDate.parse(dateStr, formatter);
					LocalDate today = LocalDate.now();
					long daysDiff = ChronoUnit.DAYS.between(fileDate, today);	
					//System.out.println("Difference in days: " + daysDiff);				
					if (daysDiff > retention_day) {
						//System.out.println("Deleting blob: " + blobName);
						containerClient.getBlobClient(blobName).delete();
					}
					*/
					
					String dateStr = matcher.group(1);
					DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
					LocalDate fileDate = null;
					try {
						fileDate = LocalDate.parse(dateStr, formatter);
					} catch (DateTimeParseException e) {
						System.err.println("Invalid date format in: " + dateStr);
						continue; // skip this iteration
					}
					if (fileDate != null) {
						LocalDate today = LocalDate.now();
						long daysDiff = ChronoUnit.DAYS.between(fileDate, today);
						if (daysDiff > retention_day) {
							containerClient.getBlobClient(blobName).delete();
						}
					}
				
				}
			}
			//System.out.println("All files in directory '" + directoryName + "' have been deleted.");
		} catch (Exception e) {
			System.err.println("Failed to delete files in directory '" + directoryName + "'. Error: " + e.getMessage());
		}
	}

    public void archiveAllFilesInDirectory(String sourceDirectory, String archiveDirectory) {
		//TokenCredential tokenCredential = new DefaultAzureCredentialBuilder().build();
        String endpoint = String.format("https://%s.blob.core.windows.net", azureblob_account);
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .credential(getTokenCredential())
                .endpoint(endpoint)
                .buildClient();
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(azureblob_container);

        if (sourceDirectory == null || sourceDirectory.isEmpty()) {
            System.err.println("Source directory must not be null or empty.");
            return;
        }
        if (!sourceDirectory.endsWith("/")) {
            sourceDirectory += "/";
        }
        if (!archiveDirectory.endsWith("/")) {
            archiveDirectory += "/";
        }

        try {
            Iterable<BlobItem> blobs = containerClient.listBlobsByHierarchy(sourceDirectory);
            for (BlobItem blobItem : blobs) {
                if (blobItem.isPrefix()) {
                    // Skip directories, process only files
                    continue;
                }
                String blobName = blobItem.getName();
                String archivedBlobName = blobName.replaceFirst(sourceDirectory, archiveDirectory); // Move file under archive directory

                // Copy blob to archive directory
                BlobClient sourceBlobClient = containerClient.getBlobClient(blobName);
                BlobClient archiveBlobClient = containerClient.getBlobClient(archivedBlobName);
                archiveBlobClient.beginCopy(sourceBlobClient.getBlobUrl(), null);
                //System.out.println("Archiving: " + blobName + " -> " + archivedBlobName);

                // Delete the original blob after copy completes
                sourceBlobClient.delete();
                //System.out.println("Deleted original file: " + blobName);
            }
        } catch (Exception e) {
            System.err.println("Failed to archive files in directory '" + sourceDirectory + "'. Error: " + e.getMessage());
        }
    }
	
}
