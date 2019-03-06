package eu.interiot.services.syntax;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;

import eu.interiot.translators.syntax.IllegalSyntaxException;
import eu.interiot.translators.syntax.SyntacticTranslator;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.*;
import org.apache.jena.vocabulary.RDF;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * INTER-IoT. Interoperability of IoT Platforms.
 * INTER-IoT is a R&D project which has received funding from the European
 * Union<92>s Horizon 2020 research and innovation programme under grant
 * agreement No 687283.
 * <p>
 * Copyright (C) 2016-2018, by (Author's company of this file):
 * - Systems Research Institute Polish Academy of Sciences
 * <p>
 * <p>
 * For more information, contact:
 * - @author <a href="mailto:pawel.szmeja@ibspan.waw.pl">Paweł Szmeja</a>
 * - Project coordinator:  <a href="mailto:coordinator@inter-iot.eu"></a>
 * <p>
 * <p>
 * This code is licensed under the EPL license, available at the root
 * application directory.
 */
public class FIWAREv2Translator extends SyntacticTranslator<String> {

    private String idURI;
    private String typeURI;
    private String valueURI;
    private String nameURI;

    private String hasAttrValueURI;

    private String attributeURI;
    private String entityURI;
    private String metadataURI;

    private String entityTypeURI;
    private String attributeTypeURI;
    private String metadataTypeURI;

    private String arrayTypeURI;
    private String elementTypeURI;
    private String hasElementURI;
    private String hasNumberURI;

    private String valueTypeURI;

    private Property hasId;
    private Property hasType;

    private Property hasValue;
    private Property hasName;
    private Property hasAttribute;
//    private Property hasEntity;

    private Property hasAttrValue;

    private Property hasMetadata;
    private Resource entityType;
    private Resource attributeType;

    private Resource metadataType;
    private Resource arrayType;
    private Resource elementType;

    private Property hasElement;
    private Property hasNumber;
    private Resource valueType;
    
    public static String FIWAREbaseURI = "http://inter-iot.eu/syntax/FIWAREv2#";
    
    protected int currentGeneratedId = 0;

    public FIWAREv2Translator() {
        super(FIWAREbaseURI, "NGSIv2");

        setIdURI(getBaseURI() + "hasId");
        setTypeURI(getBaseURI() + "hasType");
        setValueURI(getBaseURI() + "hasValue");
        setNameURI(getBaseURI() + "hasName");

        setHasAttrValueURI(getBaseURI() + "hasAttrValue");
        setAttributeURI(getBaseURI() + "hasAttribute");
        setEntityURI(getBaseURI() + "hasEntity");
        setMetadataURI(getBaseURI() + "hasMetadata");

        setEntityTypeURI(getBaseURI() + "Entity");
        setAttributeTypeURI(getBaseURI() + "Attribute");
        setMetadataTypeURI(getBaseURI() + "Metadata");

        setArrayTypeURI(getBaseURI() + "Array");
        setElementTypeURI(getBaseURI() + "ArrayElement");
        setHasElementURI(getBaseURI() + "hasElement");
        setHasNumberURI(getBaseURI() + "hasNumber");

        setValueTypeURI(getBaseURI() + "Value");

        Model jenaModel = ModelFactory.createDefaultModel();

        hasId = jenaModel.createProperty(getIdURI());
        hasType = jenaModel.createProperty(getTypeURI());
        hasValue = jenaModel.createProperty(getValueURI());
        hasName = jenaModel.createProperty(getNameURI());

        hasAttrValue = jenaModel.createProperty(getHasAttrValueURI());

        hasAttribute = jenaModel.createProperty(getAttributeURI());
//        hasEntity = jenaModel.createProperty(getEntityURI());
        hasMetadata = jenaModel.createProperty(getMetadataURI());

        entityType = jenaModel.createResource(getEntityTypeURI());
        attributeType = jenaModel.createResource(getAttributeTypeURI());
        metadataType = jenaModel.createResource(getMetadataTypeURI());

        arrayType = jenaModel.createResource(getArrayTypeURI());
        elementType = jenaModel.createResource(getElementTypeURI());
        hasElement = jenaModel.createProperty(getHasElementURI());
        hasNumber = jenaModel.createProperty(getHasNumberURI());

        valueType = jenaModel.createResource(getValueTypeURI());
    }

    @Override
    public Model toJenaModel(String formatXString) throws IOException {

        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        JsonParser parser = factory.createParser(formatXString);
        JsonNode topLevelNode = mapper.readTree(parser);

        Model jenaModel = ModelFactory.createDefaultModel();
        
        // If there is no top leve node, return the default model
        if (topLevelNode == null) return jenaModel;

        if (topLevelNode.isObject()) {
            Resource myEntity = jenaModel.createResource(FIWAREbaseURI + "_" + (currentGeneratedId++));
            parseJSONObjectToJena(myEntity, topLevelNode, jenaModel);
        } else if (topLevelNode.isArray()) {
            Resource arrayResource = jenaModel.createResource(FIWAREbaseURI + "_" + (currentGeneratedId++));
            arrayResource.addProperty(RDF.type, arrayType);
            parseArrayToJena(arrayResource, topLevelNode, jenaModel);
        } else if (topLevelNode.isValueNode()) {
            Resource valueResource = jenaModel.createResource(FIWAREbaseURI + "_" + (currentGeneratedId++));
            valueResource.addProperty(RDF.type, valueType);
            parseValueToJena(valueResource, topLevelNode, false, jenaModel);
//            valueResource.addProperty(hasValue, topLevelNode.asText());
        }

        return jenaModel;
    }

    private void parseJSONObjectToJena(Resource objectResource, JsonNode objectNode, Model jenaModel) {
        Iterator<Map.Entry<String, JsonNode>> it = objectNode.fields();

        while (it.hasNext()) {
            Map.Entry<String, JsonNode> field = it.next();
            if (field.getKey().equals("id")) {
                //Entity has to have an id
                objectResource.addProperty(RDF.type, entityType);
                objectResource.addProperty(hasId, field.getValue().asText());
            } else if (field.getKey().equals("type")) {
                objectResource.addProperty(hasType, field.getValue().asText());
            } else if (field.getKey().equals("name")) {
                objectResource.addProperty(hasName, field.getValue().asText());
            } else if (field.getKey().equals("value")) {
                parseValueToJena(objectResource, field.getValue(), true, jenaModel);
            } else if (field.getKey().equals("metadata")) {
                //Add metadata to object
                Resource jenaMetadata = jenaModel.createResource(FIWAREbaseURI + "_" + (currentGeneratedId++));
                jenaMetadata.addProperty(RDF.type, metadataType);
                objectResource.addProperty(hasMetadata, jenaMetadata);

                parseJSONObjectToJena(jenaMetadata, field.getValue(), jenaModel);
            } else {
                //Add attribute to object
                Resource attr = jenaModel.createResource(FIWAREbaseURI + "_" + (currentGeneratedId++));
                attr.addProperty(RDF.type, attributeType);
                objectResource.addProperty(hasAttribute, attr);
                attr.addProperty(hasName, field.getKey());

                parseValueToJena(attr, field.getValue(), false, jenaModel);
//                parseJSONObjectToJena(attr, field.getValue(), jenaModel);
            }
        }
    }

    private void parseValueToJena(Resource res, JsonNode jsonNode, boolean isAttrValue, Model jenaModel) {
        Property hasValProperty;
        if (isAttrValue) {
            hasValProperty = hasAttrValue;
        } else {
            hasValProperty = hasValue;
        }

        //If its a data value
        if (jsonNode.isValueNode()) {
            if (jsonNode.isNumber()) {
                //Numeric value
                if (jsonNode.isInt()) {
                    res.addLiteral(hasValProperty, new Integer(jsonNode.asInt()));
                } else if (jsonNode.isLong()) {
                    res.addLiteral(hasValProperty, jsonNode.asLong());
                } else if (jsonNode.isFloat() || jsonNode.isFloatingPointNumber()) {
                    res.addLiteral(hasValProperty, new Float(jsonNode.asDouble()));
                } else {
                    res.addLiteral(hasValProperty, jsonNode.asDouble());
                }
            } else if (jsonNode.isBoolean()) {
                //Boolean value
                res.addLiteral(hasValProperty, jsonNode.asBoolean());
            } else if (jsonNode.isTextual()) {
                //Textual value
                res.addLiteral(hasValProperty, jsonNode.asText());
            } else {
                //Not-typed (string) value
                res.addProperty(hasValProperty, jsonNode.asText());
            }
//            res.addLiteral(hasValProperty, jsonNode.asText());
        } else if (jsonNode.isArray()) {
            //If its an array of values
            Resource arrayResource = jenaModel.createResource(FIWAREbaseURI + "_" + (currentGeneratedId++));
            arrayResource.addProperty(RDF.type, arrayType);
            res.addProperty(hasValProperty, arrayResource);

            parseArrayToJena(arrayResource, jsonNode, jenaModel);
        } else if (jsonNode.isObject()) {
            //If its a JSON object
            Resource objectResource = jenaModel.createResource(FIWAREbaseURI + "_" + (currentGeneratedId++));
            //TODO: Add type to object resource? Is this necessary?
//            objectResource.addProperty(RDF.type, "xxxxxx");
            res.addProperty(hasValProperty, objectResource);
            parseJSONObjectToJena(objectResource, jsonNode, jenaModel);
        }
    }

    private void parseArrayToJena(Resource arrayResource, JsonNode arrayNode, Model jenaModel) {
        int counter = 1;
        Iterator<JsonNode> arrayIt = arrayNode.elements();
        while (arrayIt.hasNext()) {
            JsonNode element = arrayIt.next();
            Resource jenaElement = jenaModel.createResource(FIWAREbaseURI + "_" + (currentGeneratedId++));
            jenaElement.addProperty(RDF.type, elementType);
            arrayResource.addProperty(hasElement, jenaElement);
            jenaElement.addLiteral(hasNumber, new Integer(counter++));
            parseValueToJena(jenaElement, element, false, jenaModel);
        }
    }

    @Override
    public String toFormatX(Model jenaModelParam) throws IllegalSyntaxException {
        Model jenaModel = ModelFactory.createDefaultModel().add(jenaModelParam);
        ObjectMapper mapper = new ObjectMapper();
        LinkedList<JsonNode> jsonNodeList = new LinkedList<JsonNode>();

        //Find the top-level RDF Entity (the entity that does not appear in Object of RDF triples)
        Query topLevelQuery = QueryFactory.create("SELECT DISTINCT ?top WHERE { ?top ?y ?z MINUS {?a ?b ?top} }");
        QueryExecution topLevelQueryExec = QueryExecutionFactory.create(topLevelQuery, jenaModel);
        ResultSet results = topLevelQueryExec.execSelect();

        while (results.hasNext()) {
            QuerySolution solution = results.next();
            RDFNode resultNode = solution.get("top");
//            System.out.println(resultNode);

            StmtIterator typesIt = jenaModel.listStatements(new SimpleSelector(resultNode.asResource(), RDF.type, (RDFNode) null));
            StmtIterator propsIt = jenaModel.listStatements(new SimpleSelector(resultNode.asResource(), null, (RDFNode) null));
            Set<Resource> types = typesIt.toSet().stream().map(x -> x.getObject().asResource()).collect(Collectors.toSet());
            Set<Property> props = propsIt.toSet().stream().map(x -> x.getPredicate()).collect(Collectors.toSet());
            if (
                //Assume that a resource that has some RDF type from FIWARE is a FIWARE object/array/value
                    types.contains(arrayType) ||
                            types.contains(valueType) ||
                            types.contains(entityType) ||
                            types.contains(metadataType) ||
                            types.contains(metadataType) ||
                            //Assume that a resource that has some of the FIWARE properties is of FIWARE type
                            props.contains(hasAttribute) ||
                            props.contains(hasAttrValue) ||
                            props.contains(hasValue) ||
                            props.contains(hasType) ||
                            props.contains(hasMetadata) ||
                            props.contains(hasName) ||
                            props.contains(hasId) ||
                            props.contains(hasElement)
                    ) {
                JsonNode someTopLevelNode = parseRDFEntityToJson(resultNode.asResource(), jenaModel, mapper);
                if (someTopLevelNode != null) {
                    jsonNodeList.add(someTopLevelNode);
                }
            }
        }

        JsonNode topLevelNode = null;
        if (jsonNodeList.isEmpty()) {
            //TODO: Throw exception "invalid RDF structure for FIWARE translation - no top-level entity found"
            throw new IllegalSyntaxException("No top-level RDF entity found (a root node in RDF graph is required in FIWAREv2 RDF)");
//            topLevelNode = mapper.createObjectNode();
        } else if (jsonNodeList.size() == 1) {
            topLevelNode = jsonNodeList.getFirst();
        } else {
            topLevelNode = mapper.createArrayNode();
            for (JsonNode node : jsonNodeList)
                ((ArrayNode) topLevelNode).add(node);
        }

        return topLevelNode.toString();

        //Pretty printing
//        try {
//            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(topLevelNode);
//        } catch (Exception e) {
//            //Log exception
//            return null;
//        }
    }

    private JsonNode parseRDFEntityToJson(Resource entityResource, Model jenaModel, ObjectMapper mapper) {
        //TODO: Implement

        //Check entity type
        StmtIterator typesIt = jenaModel.listStatements(new SimpleSelector(entityResource, RDF.type, (RDFNode) null));
        while (typesIt.hasNext()) {
            Statement stmt = typesIt.next();
            Resource type = stmt.getObject().asResource();

            if (type.equals(entityType)) {
                ObjectNode jsonNode = mapper.createObjectNode();
//                typesIt.remove();
                parseAttributesToJson(entityResource, jsonNode, jenaModel, mapper);
                return jsonNode;

            } else if (type.equals(arrayType)) {
                ArrayNode jsonNode = parseArrayEntityToJsonArray(entityResource, jenaModel, mapper);
//                typesIt.remove();
                return jsonNode;

            } else if (type.equals(valueType)) {
                //Parse value
                StmtIterator valuesIt = jenaModel.listStatements(entityResource, hasValue, (String) null);
                while (valuesIt.hasNext()) {
                    Statement valStmt = valuesIt.next();

                    if (valStmt.getObject().isLiteral()) {
                        ValueNode jsonNode = parseLiteralToValueNode(valStmt.getObject().asLiteral(), mapper);
//                        typesIt.remove();
//                        valuesIt.remove();
                        return jsonNode;
                    }
                }
            }
        }

        //Do we assume object node at this point?
        //No type - just parse properties (and type, id, name, value and metadata) and add them as attributes
        ObjectNode jsonNode = mapper.createObjectNode();
//                typesIt.remove();
        parseAttributesToJson(entityResource, jsonNode, jenaModel, mapper);
        return jsonNode;
    }

    private void parseAttributesToJson(Resource entityResource, ObjectNode entity, Model jenaModel, ObjectMapper mapper) {

        //TODO: Test this

        //Parse id
        NodeIterator nodeIterator = jenaModel.listObjectsOfProperty(entityResource, hasId);
        if (nodeIterator.hasNext()) {
            String strFilteredId = filterThingID(nodeIterator.next().toString());
        	entity.put("id", strFilteredId);
        }

        //Parse type
        nodeIterator = jenaModel.listObjectsOfProperty(entityResource, hasType);
        if (nodeIterator.hasNext()) {
            entity.put("type", nodeIterator.next().toString());
        }

        //Parse name
        //hasName has different meaning if it is attached to an attribute, than it has for any other object type
        //We parse it separately for attributes
        if (!entityResource.hasProperty(RDF.type, attributeType)) {

            nodeIterator = jenaModel.listObjectsOfProperty(entityResource, hasName);
            if (nodeIterator.hasNext()) {
                entity.put("name", nodeIterator.next().toString());
            }
        }

        //Parse metadata
        nodeIterator = jenaModel.listObjectsOfProperty(entityResource, hasMetadata);
        if (nodeIterator.hasNext()) {
            ObjectNode metadataNode = mapper.createObjectNode();
            entity.set("metadata", metadataNode);
            parseAttributesToJson(nodeIterator.next().asResource(), metadataNode, jenaModel, mapper);
        }

        //TODO: FIXME: Something is wrong with parsing "hasValue" of attributes, and objects that are not attributes
        nodeIterator = jenaModel.listObjectsOfProperty(entityResource, hasAttrValue);
        if (nodeIterator.hasNext()) {
            RDFNode valueNode = nodeIterator.next();

            if (valueNode.isLiteral()) {
                ValueNode jsonValueNode = parseLiteralToValueNode(valueNode.asLiteral(), mapper);
                entity.set("value", jsonValueNode);
            } else if (valueNode.isResource()) {
                JsonNode jsonNode = parseRDFEntityToJson(valueNode.asResource(), jenaModel, mapper);
                entity.set("value", jsonNode);
            }
        }

        int i = 1;
        NodeIterator it = jenaModel.listObjectsOfProperty(entityResource, hasAttribute);
        while (it.hasNext()) {
            RDFNode attribute = it.next();
            //Check if it really is an attribute
            if (attribute.isResource() && attribute.asResource().hasProperty(RDF.type, attributeType)) {

                String attributeName = "";
                //Parse attribute name
                NodeIterator names = jenaModel.listObjectsOfProperty(attribute.asResource(), hasName);
                if (names.hasNext()) {
                    attributeName = names.next().toString();
                } else {
                    attributeName = "anonymous_attribute_" + i++;
                }

                NodeIterator valIterator = jenaModel.listObjectsOfProperty(attribute.asResource(), hasValue);
                if (valIterator.hasNext()) {
                    RDFNode valueNode = valIterator.next();

                    if (valueNode.isResource()) {
//                        JsonNode jsonNode = parseRDFEntityToJson(valueNode.asResource(), jenaModel, mapper);
                        ObjectNode attributeNode = mapper.createObjectNode();
//                        entity.set(attributeName, attributeNode);
                        entity.set(attributeName, parseRDFEntityToJson(valueNode.asResource(), jenaModel, mapper));
//                        parseAttributesToJson(valueNode.asResource(), attributeNode, jenaModel, mapper);
                    } else if (valueNode.isLiteral()) {
                        ValueNode jsonValueNode = parseLiteralToValueNode(valueNode.asLiteral(), mapper);
                        entity.set(attributeName, jsonValueNode);
                    }
                } else {
                    ObjectNode attributeNode = mapper.createObjectNode();
                    entity.set(attributeName, attributeNode);
                    parseAttributesToJson(attribute.asResource(), attributeNode, jenaModel, mapper);
                }
            }
        }

    }

    private ArrayNode parseArrayEntityToJsonArray(Resource arrayResource, Model jenaModel, ObjectMapper mapper) {
        //TODO: Finish this
        ArrayNode jsonArrayNode = mapper.createArrayNode();

        //Cannot ask for a binding to a blank node :(
//        String queryString = String.format("SELECT DISTINCT ?index ?value \n{ \n" +
//                        "?array <%s> ?element . \n" +
//                        "?element <%s> ?index . \n" +
//                        "?element <%s> ?value . \n" +
//                        "BIND (<%s> AS ?array)\n}",
//                hasElement, hasNumber, hasValue, arrayResource);

        StmtIterator arrayElementsIt = jenaModel.listStatements(new SimpleSelector(arrayResource, hasElement, (RDFNode) null));
        //We need a map, because elements may arrive in any order
        TreeMap<Integer, JsonNode> arrayElements = new TreeMap<Integer, JsonNode>();
        while (arrayElementsIt.hasNext()) {
            Statement stmt = arrayElementsIt.next();
            if (stmt.getObject().isResource()) {
                //Get index
                Optional<Integer> i = Optional.empty();
                StmtIterator indexIterator = jenaModel.listStatements(new SimpleSelector(stmt.getObject().asResource(), hasNumber, (String) null));
                if (indexIterator.hasNext()) {
                    RDFNode indexNode = indexIterator.next().getObject();
                    if (indexNode.isLiteral()) {
                        i = Optional.of(indexNode.asLiteral().getInt());
                    }
                }
                //Get value
                Optional<JsonNode> elementValueNode = Optional.empty();
                StmtIterator valueIterator = jenaModel.listStatements(new SimpleSelector(stmt.getObject().asResource(), hasValue, (RDFNode) null));
                if (valueIterator.hasNext()) {
                    RDFNode valueNode = valueIterator.next().getObject();
                    if (valueNode.isLiteral()) {
                        elementValueNode = Optional.of(parseLiteralToValueNode(valueNode.asLiteral(), mapper));
                    } else if (valueNode.isResource()) {
                        elementValueNode = Optional.of(parseRDFEntityToJson(valueNode.asResource(), jenaModel, mapper));
                    }

                }

                if (i.isPresent() && elementValueNode.isPresent()) {
                    arrayElements.put(i.get(), elementValueNode.get());
                }
            }
        }

        //TODO: Test and check if this is ALWAYS sorted
        Iterator<java.util.Map.Entry<Integer, JsonNode>> it = arrayElements.entrySet().iterator();
        while (it.hasNext()) {
            jsonArrayNode.add(it.next().getValue());
        }

        return jsonArrayNode;
    }

    private ValueNode parseLiteralToValueNode(Literal literal, ObjectMapper mapper) {
        //TODO: Maybe add more datatypes

        RDFDatatype datatype = literal.getDatatype();
        if (datatype != null) {
            if (datatype.equals(XSDDatatype.XSDboolean)) {
                return mapper.getNodeFactory().booleanNode(literal.getBoolean());
            }

            if (datatype.equals(XSDDatatype.XSDint) || datatype.equals(XSDDatatype.XSDinteger)) {
                return mapper.getNodeFactory().numberNode(literal.getInt());
            }

            if (datatype.equals(XSDDatatype.XSDlong)) {
                return mapper.getNodeFactory().numberNode(literal.getLong());
            }

            if (datatype.equals(XSDDatatype.XSDfloat)) {
                return mapper.getNodeFactory().numberNode(literal.getFloat());
            }

            if (datatype.equals(XSDDatatype.XSDdouble)) {
                return mapper.getNodeFactory().numberNode(literal.getDouble());
            }
            return mapper.getNodeFactory().textNode(literal.getString());
        }

        return mapper.getNodeFactory().textNode(literal.getValue().toString());
    }

    /**
     * Takes in a JSON string and attempts to make it pretties by adding spaces, indentation and new lines
     *
     * @param jsonString
     * @return
     * @throws IOException
     */
    public String prettifyJsonString(String jsonString) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        JsonParser parser = factory.createParser(jsonString);
        JsonNode jsonNode = mapper.readTree(parser);
        return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonNode);
    }

    public String getIdURI() {
        return idURI;
    }

    public void setIdURI(String idURI) {
        this.idURI = idURI;
    }

    public String getTypeURI() {
        return typeURI;
    }

    public void setTypeURI(String typeURI) {
        this.typeURI = typeURI;
    }

    public String getValueURI() {
        return valueURI;
    }

    public void setValueURI(String valueURI) {
        this.valueURI = valueURI;
    }

    public String getNameURI() {
        return nameURI;
    }

    public void setNameURI(String nameURI) {
        this.nameURI = nameURI;
    }

    public String getAttributeURI() {
        return attributeURI;
    }

    public void setAttributeURI(String attributeURI) {
        this.attributeURI = attributeURI;
    }

    public String getEntityURI() {
        return entityURI;
    }

    public void setEntityURI(String entityURI) {
        this.entityURI = entityURI;
    }

    public String getMetadataURI() {
        return metadataURI;
    }

    public void setMetadataURI(String metadataURI) {
        this.metadataURI = metadataURI;
    }

    public String getEntityTypeURI() {
        return entityTypeURI;
    }

    public void setEntityTypeURI(String entityTypeURI) {
        this.entityTypeURI = entityTypeURI;
    }

    public String getAttributeTypeURI() {
        return attributeTypeURI;
    }

    public void setAttributeTypeURI(String attributeTypeURI) {
        this.attributeTypeURI = attributeTypeURI;
    }

    public String getMetadataTypeURI() {
        return metadataTypeURI;
    }

    public void setMetadataTypeURI(String metadataTypeURI) {
        this.metadataTypeURI = metadataTypeURI;
    }

    public String getArrayTypeURI() {
        return arrayTypeURI;
    }

    public void setArrayTypeURI(String arrayTypeURI) {
        this.arrayTypeURI = arrayTypeURI;
    }

    public String getElementTypeURI() {
        return elementTypeURI;
    }

    public void setElementTypeURI(String elementTypeURI) {
        this.elementTypeURI = elementTypeURI;
    }

    public String getHasElementURI() {
        return hasElementURI;
    }

    public void setHasElementURI(String hasElementURI) {
        this.hasElementURI = hasElementURI;
    }

    public String getHasNumberURI() {
        return hasNumberURI;
    }

    public void setHasNumberURI(String hasNumberURI) {
        this.hasNumberURI = hasNumberURI;
    }

    public String getValueTypeURI() {
        return valueTypeURI;
    }

    public void setValueTypeURI(String valueTypeURI) {
        this.valueTypeURI = valueTypeURI;
    }

    public String getHasAttrValueURI() {
        return hasAttrValueURI;
    }

    public void setHasAttrValueURI(String hasAttrValueURI) {
        this.hasAttrValueURI = hasAttrValueURI;
    }
    
    // From class OrionV2Utils in Fiware Brdige
    public String filterThingID(String thingId) {
    	String filteredString = thingId;
		// Check algorithm is optimal+
    	if (filteredString.contains("http://inter-iot.eu/dev/")) {
			filteredString = thingId.replace("http://inter-iot.eu/dev/", "");
		} 
    	if (filteredString.contains("http://")) {
			filteredString = filteredString.replace("://", "_");
		}
		if (filteredString.contains("/")) {
			filteredString = filteredString.replace("/", "");
		}
		if (filteredString.contains("#")) {
			filteredString = filteredString.replace("#", "+");
		}
		if (filteredString.contains(":")) {
			filteredString = filteredString.replace(":", "");
		}
		return filteredString;
	}
}
