package com.griddynamics.training.vk.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.stream.Collectors;

@Description(
        name="IpToGeoIdUDF",
        value="checks if the given IP address belong to the given subnet"
)
public class IpToGeoIdUDF
       /* extends UDF*/ {

    private List<IpSubnetMask> subnetMasks;

    public IpToGeoIdUDF() {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(IpToGeoIdUDF.class.getResourceAsStream("/GeoLite2-Country-Blocks-IPv4.csv")))) {
            subnetMasks = reader.lines().skip(1)
                    .map(s -> s.split(","))
                    .filter(v -> v.length > 2 && v[0].length() > 0 && v[1].length() > 0)
                    .map(v -> new IpSubnetMask(v[0], Integer.parseInt(v[1])))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public int evaluate(String ipAddress) {
        int ipAddressToCheck = IpSubnetMask.ipAddressToInt(ipAddress);
        for (IpSubnetMask ipSubnetMask : subnetMasks) {
            if (ipSubnetMask.matches(ipAddressToCheck)) {
                return ipSubnetMask.getGeoNameId();
            }
        }
        return -1;
    }

    public static class IpSubnetMask {

        private int address;
        private int mask;
        private int geoNameId;

        public IpSubnetMask(String subnet, int geoNameId) {
            String[] parts = subnet.split("/");
            String ip = parts[0];
            int prefix;

            if (parts.length < 2) {
                prefix = 0;
            } else {
                prefix = Integer.parseInt(parts[1]);
            }

            this.address = ipAddressToInt(ip);
            this.mask = -(1 << (32 - prefix));
            this.geoNameId = geoNameId;
        }

        public static int ipAddressToInt(String ipAddress) {
            Inet4Address a1 =null;
            try {
                a1 = (Inet4Address) InetAddress.getByName(ipAddress);
            } catch (UnknownHostException e){}

            if (a1 == null) {
                return 0;
            }

            byte[] b1 = a1.getAddress();
            return ((b1[0] & 0xFF) << 24) |
                    ((b1[1] & 0xFF) << 16) |
                    ((b1[2] & 0xFF) << 8)  |
                    ((b1[3] & 0xFF) << 0);
        }

        public boolean matches(int addressToCheck) {
            return (address & mask) == (addressToCheck & mask);
        }

        public int getGeoNameId() {
            return geoNameId;
        }

        @Override
        public String toString() {
            return "IpSubnetMask{" + "address=" + address +
                    ", mask=" + mask +
                    ", geoNameId=" + geoNameId +
                    '}';
        }
    }
}
