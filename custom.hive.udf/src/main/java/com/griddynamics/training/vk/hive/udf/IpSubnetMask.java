package com.griddynamics.training.vk.hive.udf;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;

public class IpSubnetMask {

    private int address;
    private int mask;
    private int geoNameId;

    public IpSubnetMask(int[] address, int prefix, int geoNameId) {
        this.address = ipAddressToInt(address);
        this.mask = -(1 << (32 - prefix));
        this.geoNameId = geoNameId;
    }

    public static Optional<Integer> ipAddressToInt(String ipAddress) {
        return getIpAddressSegments(ipAddress).map(IpSubnetMask::ipAddressToInt);
    }

    public static int ipAddressToInt(int[] address) {
        return address[0] << 24 | address[1] << 16 | address[2] << 8 | address[3];
    }

    public static Optional<int[]> getIpAddressSegments(String ipAddress) {
        try {
            int[] ipSegments = new int[4];
            int index = 0;
            for (byte b : InetAddress.getByName(ipAddress).getAddress()) {
                ipSegments[index++] = b & 0xFF;
            }
            return Optional.of(ipSegments);
        } catch (UnknownHostException e) {
            return Optional.empty();
        }
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
