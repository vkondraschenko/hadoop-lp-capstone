package com.griddynamics.training.vk.hive.udf;

import java.util.*;

public class IpTreeEntry {

    private Map<Integer, IpTreeEntry> leafs = new HashMap<>();
    private List<IpSubnetMask> subnetMasks = new ArrayList<>();

    public IpTreeEntry addLeafIfAbsent(int leafValue) {
        return this.leafs.computeIfAbsent(leafValue, k -> new IpTreeEntry());
    }

    public void addSubnetMask(IpSubnetMask ipSubnetMask) {
        this.subnetMasks.add(ipSubnetMask);
    }

    public Optional<IpTreeEntry> findLeaf(int ipSegmentValue) {
        return Optional.ofNullable(leafs.get(ipSegmentValue));
    }

    public Optional<Integer> findMatchingGeoNameId(int addressToCheck) {
        for (IpSubnetMask ipSubnetMask : subnetMasks) {
            if (ipSubnetMask.matches(addressToCheck)) {
                return Optional.of(ipSubnetMask.getGeoNameId());
            }
        }
        return Optional.empty();
    }
}
