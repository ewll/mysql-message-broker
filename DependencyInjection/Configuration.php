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
        $treeBuilder = new TreeBuilder();
        $rootNode = $treeBuilder->root('ewll_mysql-message-broker');

        $rootNode
            ->children()
        ;

        return $treeBuilder;
    }
}
