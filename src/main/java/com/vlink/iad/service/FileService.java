package com.vlink.iad.service;

import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

@Service
public class FileService {

    public List<String> getFilteredFiles(String directoryPath, String filterPattern) throws IOException {
        List<String> filteredFiles = new ArrayList<>();
        Path dirPath = Paths.get(directoryPath);

        if (Files.exists(dirPath) && Files.isDirectory(dirPath)) {
            DirectoryStream<Path> directoryStream = Files.newDirectoryStream(dirPath, entry -> {
                // Case-insensitive match for the filter pattern
                String fileName = entry.getFileName().toString().toLowerCase();
                return fileName.matches(filterPattern.toLowerCase());
            });

            for (Path file : directoryStream) {
                filteredFiles.add(file.toAbsolutePath().toString());
            }
        }
        return filteredFiles;
    }
}
