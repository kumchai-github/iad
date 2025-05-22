package com.vlink.iad.runner;

import com.vlink.iad.service.CsvImportService;
import com.vlink.iad.service.CsvExportService;
import com.vlink.iad.service.AzureBlobService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.stream.Stream;

@Component
public class TransformRunner implements CommandLineRunner {

	@Value("${log.directory}")
	private String log_directory;
	@Value("${log.archived.directory}")
	private String log_archived_directory;	
	@Value("${output.directory}")
	private String output_directory;
	@Value("${output.archived.directory}")
	private String output_archived_directory;	
	@Value("${input.directory}")
	private String input_directory;		
	@Value("${input.archived.directory}")
	private String input_archived_directory;
	
    private final CsvImportService csvImportService;	
    private final CsvExportService csvExportService;
	private final AzureBlobService azureblobService;
		
    @Autowired
    public TransformRunner(
		CsvImportService csvImportService,			
		CsvExportService csvExportService,
		AzureBlobService azureblobService		
		) 
	{
        this.csvImportService = csvImportService;			
        this.csvExportService = csvExportService;
		this.azureblobService = azureblobService;		
    }

    @Override
    public void run(String... args) throws Exception {
		if (args.length==1) {
			if (args[0].equalsIgnoreCase("clean")) {
				csvExportService.deleteAllFilesInOutput();
				csvExportService.deleteAllFilesInLog();			
				csvImportService.deleteAllOfferings();				
				csvImportService.deleteAllRegistrations();
			} else {
				System.out.println("Invalid arguments");
			}
		} else if (args.length==2) {
			if (args[0].equalsIgnoreCase("import")) {
				if (args[1].equalsIgnoreCase("offering")) {
					csvImportService.importOfferingEntity();
				} else if (args[1].equalsIgnoreCase("registration")) {
					csvImportService.importRegistrationEntity();
				} else {
					System.out.println("Invalid arguments");
				}
			} else if (args[0].equalsIgnoreCase("export")) {
				if (args[1].equalsIgnoreCase("course")) {
					csvExportService.exportCourseCsv(false);
				} else if (args[1].equalsIgnoreCase("class")) {
					csvExportService.exportClassCsv(false);
				} else if (args[1].equalsIgnoreCase("registration")) {
					csvExportService.exportRegistrationCsv(false);
				} else if (args[1].equalsIgnoreCase("transcript")) {
					csvExportService.exportTranscriptCsv(false);
				} else {
					System.out.println("Invalid arguments");
				}
			} else if (args[0].equalsIgnoreCase("get")) {
				if (args[1].equalsIgnoreCase("data")) {
					azureblobService.getData();
				} else if (args[1].equalsIgnoreCase("log")) {
					azureblobService.getLog();
				} else {
					System.out.println("Invalid arguments");
				}				
			} else if (args[0].equalsIgnoreCase("put")) {
				if (args[1].equalsIgnoreCase("data")) {
					azureblobService.putData(false);
				} else if (args[1].equalsIgnoreCase("log")) {
					azureblobService.putLog(false, false);
				} else {
					System.out.println("Invalid arguments");
				}				
			} else if (args[0].equalsIgnoreCase("view")) {
				if (args[1].equalsIgnoreCase("offering")) {
					csvImportService.viewOffering();
				} else if (args[1].equalsIgnoreCase("registration")) {
					csvImportService.viewRegistration();
				} else {
					System.out.println("Invalid arguments");
				}
			} else {
				System.out.println("Invalid arguments");				
			}
		} else if (args.length==3) {
			if (args[0].equalsIgnoreCase("export")) {
				if (args[1].equalsIgnoreCase("course")) {
					if (args[2].equalsIgnoreCase("encrypt")) {
						csvExportService.exportCourseCsv(true);
					} else {
						System.out.println("Invalid arguments");
					}
				} else if (args[1].equalsIgnoreCase("class")) {
					if (args[2].equalsIgnoreCase("encrypt")) {
						csvExportService.exportClassCsv(true);
					} else {
						System.out.println("Invalid arguments");
					}
				} else if (args[1].equalsIgnoreCase("registration")) {
					if (args[2].equalsIgnoreCase("encrypt")) {
						csvExportService.exportRegistrationCsv(true);
					} else {
						System.out.println("Invalid arguments");
					}
				} else if (args[1].equalsIgnoreCase("transcript")) {
					if (args[2].equalsIgnoreCase("encrypt")) {
						csvExportService.exportTranscriptCsv(true);
					} else {
						System.out.println("Invalid arguments");
					}					
				} else {
					System.out.println("Invalid arguments");
				}
			} else if (args[0].equalsIgnoreCase("put")) {
				if (args[1].equalsIgnoreCase("log")) {
					if (args[2].equalsIgnoreCase("datestamp")) {
						azureblobService.putLog(true, false);
					} else {						
						System.out.println("Invalid arguments");
					}
				} else if (args[1].equalsIgnoreCase("data")) {
					if (args[2].equalsIgnoreCase("init")) {
						azureblobService.putData(true);
					} else {
						System.out.println("Invalid arguments");
					}
				} else if (args[1].equalsIgnoreCase("map")) {
					if (args[2].equalsIgnoreCase("init")) {
						azureblobService.putMap();
					} else {
						System.out.println("Invalid arguments");
					}
				} else {
					System.out.println("Invalid arguments");
				}
			} else if (args[0].equalsIgnoreCase("clean")) {	
				if (args[1].equalsIgnoreCase("blob")) {
					if (args[2].equalsIgnoreCase("output")) {
						azureblobService.deleteAllFilesInDirectory(output_directory);
					} else if (args[2].equalsIgnoreCase("log")) {
						azureblobService.deleteAllFilesInDirectory(log_directory);
					} else {
						System.out.println("Invalid arguments");
					}
				} else {
					System.out.println("Invalid arguments");
				}
			} else if (args[0].equalsIgnoreCase("archive")) {
				if (args[1].equalsIgnoreCase("blob")) {
					if (args[2].equalsIgnoreCase("input")) {
						azureblobService.archiveAllFilesInDirectory(input_directory, input_archived_directory);
					} else if (args[2].equalsIgnoreCase("output")) {
						azureblobService.archiveAllFilesInDirectory(output_directory, output_archived_directory);
					} else if (args[2].equalsIgnoreCase("log")) {
						azureblobService.archiveAllFilesInDirectory(log_directory, log_archived_directory);
					} else {
						System.out.println("Invalid arguments");
					}
				} else {
					System.out.println("Invalid arguments");
				}			
			} else {
				System.out.println("Invalid arguments");
			}
		} else if (args.length==4) {
			if (args[0].equalsIgnoreCase("put")) {
				if (args[1].equalsIgnoreCase("log")) {
					if (args[2].equalsIgnoreCase("datestamp")) {
						if (args[3].equalsIgnoreCase("encrypt")) {
							azureblobService.putLog(true, true);
						} else {
							System.out.println("Invalid arguments");
						}
					} else {
						System.out.println("Invalid arguments");
					}
				} else {
					System.out.println("Invalid arguments");
				}
			} if (args[0].equalsIgnoreCase("delete")) {
				if (args[1].equalsIgnoreCase("blob")) {
					int retention_day = Integer.parseInt(args[3]);
					if (args[2].equalsIgnoreCase("input")) {
						if ((retention_day >= 0)&& (retention_day <= 365)) {
							azureblobService.deleteFilesInDirectory(input_archived_directory, retention_day);
						} else {
							System.out.println("Invalid arguments");
						}						
					} else if (args[2].equalsIgnoreCase("output")){ 
						if ((retention_day >= 0)&& (retention_day <= 365)) {
							azureblobService.deleteFilesInDirectory(output_archived_directory, retention_day);
						} else {
							System.out.println("Invalid arguments");
						}						
					} else if (args[2].equalsIgnoreCase("log")){ 
						if ((retention_day >= 0)&& (retention_day <= 365)) {
							azureblobService.deleteFilesInDirectory(output_archived_directory, retention_day);
						} else {
							System.out.println("Invalid arguments");
						}						
					} else {
						System.out.println("Invalid arguments");
					}
				} else {
					System.out.println("Invalid arguments");
				}				
			} else {
				System.out.println("Invalid arguments");
			}
		} else {
			System.out.println("Invalid arguments");			
		}
		System.exit(0);
    }
}
