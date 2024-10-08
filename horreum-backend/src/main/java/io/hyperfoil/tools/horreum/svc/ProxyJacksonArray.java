package io.hyperfoil.tools.horreum.svc;

import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyArray;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class ProxyJacksonArray implements ProxyArray {

    private ArrayNode node;

    public ProxyJacksonArray(ArrayNode node) {
        this.node = node;
    }

    public ArrayNode getJsonNode() {
        return node;
    }

    @Override
    public Object get(long index) {
        return Util.convertFromJson(node.get((int) index));
    }

    @Override
    public void set(long index, Value value) {
        Object converted = Util.convert(value);
        node.set((int) index, new ObjectMapper().valueToTree(converted));
    }

    @Override
    public boolean remove(long index) {
        return node.remove((int) index) != null;
    }

    @Override
    public long getSize() {
        return node.size();
    }
}
