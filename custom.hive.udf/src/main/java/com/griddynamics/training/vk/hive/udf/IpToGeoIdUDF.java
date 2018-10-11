package com.griddynamics.training.vk.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Optional;

@Description(
        name="IpToGeoIdUDF",
        value="checks if the given IP address belong to the given subnet"
)
public class IpToGeoIdUDF
        extends UDF {

    private IpTreeEntry ipTreeRoot = new IpTreeEntry();

    public IpToGeoIdUDF() {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(IpToGeoIdUDF.class.getResourceAsStream("/GeoLite2-Country-Blocks-IPv4.csv")))) {
            reader.lines().skip(1)
                    .map(s -> s.split(","))
                    .filter(v -> v.length > 2 && v[0].length() > 0 && v[1].length() > 0)
                    .forEach(v -> processIpMask(v[0], Integer.parseInt(v[1])));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void processIpMask(String subnet, int geoNameId) {
        String[] parts = subnet.split("/");
        int[] ipSegments = IpSubnetMask.getIpAddressSegments(parts[0]).orElse(null);
        int prefix = parts.length < 2 ? 0 : Integer.parseInt(parts[1]);

        if (ipSegments == null || prefix == 0) {
            return;
        }

        IpSubnetMask ipSubnetMask = new IpSubnetMask(ipSegments, prefix, geoNameId);
        if (prefix < 8) {
            ipTreeRoot.addSubnetMask(ipSubnetMask);
        } else if (prefix < 16) {
            ipTreeRoot.addLeafIfAbsent(ipSegments[0]).addSubnetMask(ipSubnetMask);
        } else if (prefix < 24) {
            ipTreeRoot.addLeafIfAbsent(ipSegments[0]).addLeafIfAbsent(ipSegments[1]).addSubnetMask(ipSubnetMask);
        } else {
            ipTreeRoot.addLeafIfAbsent(ipSegments[0]).addLeafIfAbsent(ipSegments[1]).addLeafIfAbsent(ipSegments[2]).addSubnetMask(ipSubnetMask);
        }
    }

    public int evaluate(String ipAddress) {
        return IpSubnetMask.getIpAddressSegments(ipAddress).map(ip -> {
            IpTreeEntry bestMatchingEntry = ipTreeRoot;
            for (int ipSegment : ip) {
                Optional<IpTreeEntry> nextLevelEntry = bestMatchingEntry.findLeaf(ipSegment);
                if (nextLevelEntry.isPresent()) {
                    bestMatchingEntry = nextLevelEntry.get();
                } else {
                    break;
                }
            }
            return bestMatchingEntry.findMatchingGeoNameId(IpSubnetMask.ipAddressToInt(ip)).orElse(-1);
        }).orElse(-1);
    }
}
