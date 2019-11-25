<?php namespace Ewll\MysqlMessageBrokerBundle\DependencyInjection;

use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\ConfigurationInterface;

/**
 * {@inheritdoc}
 */
class Configuration implements ConfigurationInterface
{
    /**
     * {@inheritdoc}
     */
    public function getConfigTreeBuilder()
    {
        $treeBuilder = new TreeBuilder('ewll_mysql_message_broker');
        $treeBuilder->getRootNode()
            ->children()
        ;

        return $treeBuilder;
    }
}
