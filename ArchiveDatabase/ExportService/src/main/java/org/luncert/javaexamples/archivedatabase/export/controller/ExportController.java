package org.luncert.javaexamples.archivedatabase.export.controller;

import lombok.RequiredArgsConstructor;
import org.luncert.javaexamples.archivedatabase.export.service.IExportService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class ExportController {

    private final IExportService exportService;

    @PostMapping("/exportDirectly")
    public void exportDirectly() {
        exportService.exportDirectly();
    }

    @PostMapping("/exportUsingDirectMemory")
    public void export() {
        exportService.exportUsingDirectMemory();
    }
}
