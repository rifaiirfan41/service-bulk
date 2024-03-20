package com.example;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.jboss.logging.Logger;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.util.List;

@Path("/csv")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class CsvResource {

    private static final Logger LOG = Logger.getLogger(CsvResource.class);

    @ConfigProperty(name = "auth.token")
    String authToken;

    @Inject
    CsvService csvService;

    @Operation(summary = "Upload CSV")
    @POST
    @Path("/upload")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response uploadCsvFile(@HeaderParam("Authorization") String token, @FormDataParam("file") InputStream fileStream) {
        if (!authenticate(token)) {
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }

        try {
            List<CsvRecord> records = csvService.processCsv(fileStream);
            csvService.publishToKafka(records);
            return Response.ok("CSV uploaded and processed successfully").build();
        } catch (Exception e) {
            LOG.error("Failed to process CSV: " + e.getMessage());
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("Failed to process CSV").build();
        }
    }

    private boolean authenticate(String token) {
        return token != null && token.equals(authToken);
    }
}
